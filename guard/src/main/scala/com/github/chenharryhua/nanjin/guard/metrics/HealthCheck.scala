package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.effect.syntax.temporal.given
import cats.syntax.functor.given
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.event.{Category, GaugeKind, MetricID, MetricLabel, MetricName}

import java.time.ZoneId
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

trait HealthCheck[F[_]]:
  def register(hc: F[Boolean]): Resource[F, Unit]

  /** heath check sometimes is expensive.
    * @param hc
    *   health check method.
    */
  def register(hc: F[Boolean], f: Policy.type => Policy): Resource[F, Unit]
end HealthCheck

object HealthCheck {
  def noop[F[_]]: HealthCheck[F] =
    new HealthCheck[F] {
      override def register(hc: F[Boolean]): Resource[F, Unit] =
        Resource.unit[F]
      override def register(hc: F[Boolean], f: Policy.type => Policy): Resource[F, Unit] =
        Resource.unit[F]
    }

  private class Impl[F[_]: Async](
    label: MetricLabel,
    metricRegistry: metrics.MetricRegistry,
    timeout: FiniteDuration,
    name: String,
    dispatcher: Dispatcher[F],
    zoneId: ZoneId)
      extends HealthCheck[F] {

    private val F = Async[F]

    override def register(hc: F[Boolean]): Resource[F, Unit] =
      for {
        metricID <- Resource.eval(MetricName(name).map { metricName =>
          MetricID(label, metricName, Category.Gauge(GaugeKind.HealthCheck)).identifier
        })
        _ <- Resource.make(
          F.delay(
            metricRegistry.gauge(
              metricID,
              () =>
                new metrics.Gauge[Boolean] {
                  override def getValue: Boolean =
                    Try(dispatcher.unsafeRunTimed(hc, timeout)).fold(_ => false, identity)
                }
            )))(_ => F.delay(metricRegistry.remove(metricID)).void)
      } yield ()

    override def register(hc: F[Boolean], f: Policy.type => Policy): Resource[F, Unit] = {
      val check: F[Boolean] = F.handleError(hc.timeout(timeout))(_ => false)
      run_gauge_job_background(check, zoneId, f).flatMap(ref => register(ref.get))
    }
  }

  final class Builder private[HealthCheck] (isEnabled: Boolean, timeout: FiniteDuration)
      extends EnableConfig[Builder] {

    def withTimeout(timeout: FiniteDuration): Builder =
      new Builder(isEnabled, timeout)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, timeout)

    private[HealthCheck] def build[F[_]: Async](
      label: MetricLabel,
      name: String,
      metricRegistry: metrics.MetricRegistry,
      dispatcher: Dispatcher[F],
      zoneId: ZoneId): HealthCheck[F] = {
      val hc: HealthCheck[F] =
        new Impl[F](label, metricRegistry, timeout, name, dispatcher, zoneId)

      if isEnabled then hc else noop[F]
    }
  }

  private[metrics] def apply[F[_]: Async](
    mr: metrics.MetricRegistry,
    label: MetricLabel,
    name: String,
    f: Endo[Builder],
    dispatcher: Dispatcher[F],
    zoneId: ZoneId): HealthCheck[F] =
    f(new Builder(isEnabled = true, timeout = 7.seconds))
      .build[F](label, name, mr, dispatcher, zoneId)
}
