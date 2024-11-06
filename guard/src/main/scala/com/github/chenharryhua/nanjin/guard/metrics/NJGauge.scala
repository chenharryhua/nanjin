package com.github.chenharryhua.nanjin.guard.metrics

import cats.effect.implicits.genTemporalOps
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.GaugeKind
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.ZoneId
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

sealed trait NJGauge[F[_]] {
  def register[A: Encoder](value: F[A]): Resource[F, Unit]
  def register[A: Encoder](value: F[A], policy: Policy, zoneId: ZoneId): Resource[F, Unit]
}

object NJGauge {
  def dummy[F[_]]: NJGauge[F] =
    new NJGauge[F] {
      override def register[A: Encoder](value: F[A]): Resource[F, Unit] =
        Resource.unit[F]
      override def register[A: Encoder](value: F[A], policy: Policy, zoneId: ZoneId): Resource[F, Unit] =
        Resource.unit[F]
    }

  private class Impl[F[_]: Async](
    private[this] val name: MetricName,
    private[this] val metricRegistry: MetricRegistry,
    private[this] val timeout: FiniteDuration,
    private[this] val tag: String
  ) extends NJGauge[F] {

    private[this] val F = Async[F]

    private[this] def trans_error(ex: Throwable): Json =
      Json.fromString(StringUtils.abbreviate(ExceptionUtils.getRootCauseMessage(ex), 80))

    private[this] def json_gauge[A: Encoder](metricID: MetricID, fa: F[A]): Resource[F, Unit] =
      Dispatcher.sequential[F].flatMap { dispatcher =>
        Resource
          .make(F.delay {
            metricRegistry.gauge(
              metricID.identifier,
              () =>
                new Gauge[Json] {
                  override def getValue: Json =
                    Try(dispatcher.unsafeRunTimed(fa, timeout)).fold(trans_error, _.asJson)
                }
            )
          })(_ => F.delay(metricRegistry.remove(metricID.identifier)).void)
          .void
      }

    override def register[A: Encoder](value: F[A]): Resource[F, Unit] =
      Resource.eval(F.monotonic).flatMap { ts =>
        val metricID: MetricID = MetricID(name, MetricTag(tag, ts), Category.Gauge(GaugeKind.Gauge))
        json_gauge(metricID, value)
      }

    override def register[A: Encoder](value: F[A], policy: Policy, zoneId: ZoneId): Resource[F, Unit] = {
      val fetch: F[Json] = value.timeout(timeout).attempt.map(_.fold(trans_error, _.asJson))
      for {
        init <- Resource.eval(fetch)
        ref <- Resource.eval(F.ref(init))
        _ <- F.background(tickStream[F](policy, zoneId).evalMap(_ => fetch.flatMap(ref.set)).compile.drain)
        _ <- register(ref.get)
      } yield ()
    }
  }

  final class Builder private[guard] (isEnabled: Boolean, timeout: FiniteDuration)
      extends EnableConfig[Builder] {

    def withTimeout(timeout: FiniteDuration): Builder =
      new Builder(isEnabled, timeout)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, timeout)

    private[guard] def build[F[_]: Async](
      metricName: MetricName,
      tag: String,
      metricRegistry: MetricRegistry): NJGauge[F] =
      if (isEnabled) {
        new Impl[F](metricName, metricRegistry, timeout, tag)
      } else dummy[F]
  }
}
