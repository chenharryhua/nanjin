package com.github.chenharryhua.nanjin.guard.metrics

import cats.effect.kernel.{Async, Concurrent, Ref, Resource}
import cats.effect.std.Dispatcher
import cats.effect.syntax.temporal.genTemporalOps
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.GaugeKind
import com.github.chenharryhua.nanjin.guard.config.{Category, MetricID, MetricLabel, MetricName}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.ZoneId
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait Gauge[F[_]] {
  def register[A: Encoder](value: F[A]): Resource[F, Unit]
  def register[A: Encoder](value: F[A], f: Policy.type => Policy): Resource[F, Unit]

  def ref[A: Encoder](value: A): Resource[F, Ref[F, A]]
}

object Gauge {
  def noop[F[_]: Concurrent]: Gauge[F] =
    new Gauge[F] {
      override def register[A: Encoder](value: F[A]): Resource[F, Unit] =
        Resource.unit[F]
      override def register[A: Encoder](value: F[A], f: Policy.type => Policy): Resource[F, Unit] =
        Resource.unit[F]
      override def ref[A: Encoder](value: A): Resource[F, Ref[F, A]] =
        Resource.eval(Concurrent[F].ref(value))
    }

  private class Impl[F[_]: Async](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: metrics.MetricRegistry,
    private[this] val timeout: FiniteDuration,
    private[this] val name: String,
    private[this] val dispatcher: Dispatcher[F],
    private[this] val zoneId: ZoneId
  ) extends Gauge[F] {

    private[this] val F = Async[F]

    private[this] def trans_error(ex: Throwable): Json =
      Json.fromString(StringUtils.abbreviate(ExceptionUtils.getRootCauseMessage(ex), 80))

    private[this] def json_gauge[A: Encoder](metricID: MetricID, fa: F[A]): Resource[F, Unit] =
      Resource
        .make(F.delay {
          metricRegistry.gauge(
            metricID.identifier,
            () =>
              new metrics.Gauge[Json] {
                override def getValue: Json =
                  Try(dispatcher.unsafeRunTimed(fa, timeout)).fold(trans_error, _.asJson)
              }
          )
        })(_ => F.delay(metricRegistry.remove(metricID.identifier)).void)
        .void

    override def register[A: Encoder](value: F[A]): Resource[F, Unit] =
      Resource
        .eval(MetricName(name).map(MetricID(label, _, Category.Gauge(GaugeKind.Gauge))))
        .flatMap(json_gauge(_, value))

    override def register[A: Encoder](value: F[A], f: Policy.type => Policy): Resource[F, Unit] = {
      val fetch: F[Json] = F.handleError(value.map(_.asJson).timeout(timeout))(trans_error)
      run_gauge_job_background(fetch, zoneId, f).flatMap(ref => register(ref.get))
    }

    override def ref[A: Encoder](value: A): Resource[F, Ref[F, A]] =
      for {
        ref <- Resource.eval(F.ref(value))
        _ <- register(ref.get)
      } yield ref
  }

  final class Builder private[guard] (isEnabled: Boolean, timeout: FiniteDuration)
      extends EnableConfig[Builder] {

    def withTimeout(timeout: FiniteDuration): Builder =
      new Builder(isEnabled, timeout)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, timeout)

    private[guard] def build[F[_]: Async](
      label: MetricLabel,
      name: String,
      metricRegistry: metrics.MetricRegistry,
      dispatcher: Dispatcher[F],
      zoneId: ZoneId): Gauge[F] = {
      val gauge: Gauge[F] =
        new Impl[F](label, metricRegistry, timeout, name, dispatcher, zoneId)

      fold_create_noop(isEnabled)(gauge, noop[F])
    }
  }
}
