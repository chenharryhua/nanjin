package com.github.chenharryhua.nanjin.guard.action

import cats.Eval
import cats.effect.implicits.genTemporalOps
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.GaugeKind
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.{Duration, ZoneId}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Try

sealed trait NJGauge[F[_]] {
  def register[A: Encoder](value: F[A]): Resource[F, Unit]
  def register[A: Encoder](value: F[A], policy: Policy, zoneId: ZoneId): Resource[F, Unit]
  def timed: Resource[F, Unit]

  def instrument[A: Encoder](value: F[A]): Resource[F, Unit]
  def instrument[A: Encoder](value: Eval[A]): Resource[F, Unit]
}

private class NJGaugeImpl[F[_]: Async](
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val timeout: FiniteDuration
) extends NJGauge[F] {

  private[this] val F = Async[F]

  private[this] def elapse(start: FiniteDuration, end: FiniteDuration): Duration =
    (end - start).toJava

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

  override def instrument[A: Encoder](value: F[A]): Resource[F, Unit] =
    Resource.eval(F.unique).flatMap { token =>
      val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.Instrument), token)
      json_gauge(metricID, value)
    }

  override def instrument[A: Encoder](value: Eval[A]): Resource[F, Unit] =
    instrument(F.catchNonFatalEval(value))

  override val timed: Resource[F, Unit] =
    Resource.eval(F.unique).flatMap { token =>
      val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.Timed), token)
      Resource.eval(F.monotonic).flatMap { kickoff =>
        json_gauge(metricID, F.monotonic.map(elapse(kickoff, _)))
      }
    }

  override def register[A: Encoder](value: F[A]): Resource[F, Unit] =
    Resource.eval(F.unique).flatMap { token =>
      val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.Gauge), token)
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

object NJGauge {

  final class Builder private[guard] (measurement: Measurement, timeout: FiniteDuration, isEnabled: Boolean) {

    def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), timeout, isEnabled)

    def withTimeout(timeout: FiniteDuration): Builder = new Builder(measurement, timeout, isEnabled)

    def enable(value: Boolean): Builder = new Builder(measurement, timeout, value)

    private[guard] def build[F[_]: Async](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams): NJGauge[F] =
      if (isEnabled) {
        val metricName = MetricName(serviceParams, measurement, name)
        new NJGaugeImpl[F](metricName, metricRegistry, timeout)
      } else
        new NJGauge[F] {
          override def register[A: Encoder](value: F[A]): Resource[F, Unit] =
            Resource.unit[F]
          override def register[A: Encoder](value: F[A], policy: Policy, zoneId: ZoneId): Resource[F, Unit] =
            Resource.unit[F]
          override def timed: Resource[F, Unit] =
            Resource.unit[F]
          override def instrument[A: Encoder](value: F[A]): Resource[F, Unit] =
            Resource.unit[F]
          override def instrument[A: Encoder](value: Eval[A]): Resource[F, Unit] =
            Resource.unit[F]
        }
  }
}
