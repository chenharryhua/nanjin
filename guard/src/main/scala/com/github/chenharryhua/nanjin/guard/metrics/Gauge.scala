package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.kernel.{Async, Concurrent, Ref, Resource}
import cats.effect.std.Dispatcher
import cats.effect.syntax.temporal.given
import cats.syntax.functor.given
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.event.{
  Category,
  GaugeKind,
  MetricID,
  MetricLabel,
  MetricName,
  StackTrace
}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

import java.time.ZoneId
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

trait Gauge[F[_]]:
  def register[A: Encoder](value: F[A]): Resource[F, Unit]
  def register[A: Encoder](value: F[A], f: Policy.type => Policy): Resource[F, Unit]

  def ref[A: Encoder](value: A): Resource[F, Ref[F, A]]
end Gauge

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
    label: MetricLabel,
    metricRegistry: metrics.MetricRegistry,
    timeout: FiniteDuration,
    name: String,
    dispatcher: Dispatcher[F],
    zoneId: ZoneId
  ) extends Gauge[F] {

    private val F = Async[F]

    private def trans_error(ex: Throwable): Json =
      StackTrace(ex).value.headOption.asJson

    private def json_gauge[A: Encoder](metricID: MetricID, fa: F[A]): Resource[F, Unit] =
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

  final class Builder private[Gauge] (isEnabled: Boolean, timeout: FiniteDuration)
      extends EnableConfig[Builder] {

    def withTimeout(timeout: FiniteDuration): Builder =
      new Builder(isEnabled, timeout)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, timeout)

    private[Gauge] def build[F[_]: Async](
      label: MetricLabel,
      name: String,
      metricRegistry: metrics.MetricRegistry,
      dispatcher: Dispatcher[F],
      zoneId: ZoneId): Gauge[F] = {
      val gauge: Gauge[F] =
        new Impl[F](label, metricRegistry, timeout, name, dispatcher, zoneId)

      if isEnabled then gauge else noop
    }
  }

  private[metrics] def apply[F[_]: Async](
    mr: metrics.MetricRegistry,
    label: MetricLabel,
    name: String,
    f: Endo[Builder],
    dispatcher: Dispatcher[F],
    zoneId: ZoneId): Gauge[F] =
    f(new Builder(isEnabled = true, timeout = 5.seconds))
      .build[F](label, name, mr, dispatcher, zoneId)
}
