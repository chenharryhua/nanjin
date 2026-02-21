package com.github.chenharryhua.nanjin.guard.metrics

import cats.effect.implicits.genTemporalOps
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.event.CategoryKind.GaugeKind
import com.github.chenharryhua.nanjin.guard.event.{Category, MetricID, MetricLabel, MetricName}

import java.time.ZoneId
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait HealthCheck[F[_]] {
  def register(hc: F[Boolean]): Resource[F, Unit]

  /** heath check sometimes is expensive.
    * @param hc
    *   health check method.
    */
  def register(hc: F[Boolean], f: Policy.type => Policy): Resource[F, Unit]
}

object HealthCheck {
  def noop[F[_]]: HealthCheck[F] =
    new HealthCheck[F] {
      override def register(hc: F[Boolean]): Resource[F, Unit] =
        Resource.unit[F]
      override def register(hc: F[Boolean], f: Policy.type => Policy): Resource[F, Unit] =
        Resource.unit[F]
    }

  private class Impl[F[_]: Async](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: metrics.MetricRegistry,
    private[this] val timeout: FiniteDuration,
    private[this] val name: String,
    private[this] val dispatcher: Dispatcher[F],
    private[this] val zoneId: ZoneId)
      extends HealthCheck[F] {

    private[this] val F = Async[F]

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
      zoneId: ZoneId): HealthCheck[F] = {
      val hc: HealthCheck[F] =
        new Impl[F](label, metricRegistry, timeout, name, dispatcher, zoneId)

      fold_create_noop(isEnabled)(hc, noop[F])
    }
  }
}
