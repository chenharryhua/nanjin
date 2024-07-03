package com.github.chenharryhua.nanjin.guard.action

import cats.effect.implicits.genTemporalOps
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.GaugeKind

import java.time.ZoneId
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

sealed trait NJHealthCheck[F[_]] {
  def register(hc: F[Boolean]): Resource[F, Unit]

  /** heath check sometimes is expensive.
    * @param hc
    *   health check method.
    */
  def register(hc: F[Boolean], policy: Policy, zoneId: ZoneId): Resource[F, Unit]
}

private class NJHealthCheckImpl[F[_]: Async](
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val timeout: FiniteDuration)
    extends NJHealthCheck[F] {

  private[this] val F = Async[F]

  override def register(hc: F[Boolean]): Resource[F, Unit] =
    Dispatcher.sequential[F].flatMap { dispatcher =>
      Resource.eval(F.unique).flatMap { token =>
        val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.HealthCheck), token)
        Resource
          .make(F.delay {
            metricRegistry.gauge(
              metricID.identifier,
              () =>
                new Gauge[Boolean] {
                  override def getValue: Boolean =
                    Try(dispatcher.unsafeRunTimed(hc, timeout)).fold(_ => false, identity)
                }
            )
          })(_ => F.delay(metricRegistry.remove(metricID.identifier)).void)
          .void
      }
    }

  override def register(hc: F[Boolean], policy: Policy, zoneId: ZoneId): Resource[F, Unit] = {
    val check: F[Boolean] = hc.timeout(timeout).attempt.map(_.fold(_ => false, identity))
    for {
      init <- Resource.eval(check)
      ref <- Resource.eval(F.ref(init))
      _ <- F.background(tickStream[F](policy, zoneId).evalMap(_ => check.flatMap(ref.set)).compile.drain)
      _ <- register(ref.get)
    } yield ()
  }
}

object NJHealthCheck {

  final class Builder private[guard] (measurement: Measurement, timeout: FiniteDuration, isEnabled: Boolean) {

    def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), timeout, isEnabled)

    def withTimeout(timeout: FiniteDuration): Builder = new Builder(measurement, timeout, isEnabled)

    def enable(value: Boolean): Builder = new Builder(measurement, timeout, value)

    private[guard] def build[F[_]: Async](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams): NJHealthCheck[F] =
      if (isEnabled) {
        val metricName = MetricName(serviceParams, measurement, name)
        new NJHealthCheckImpl[F](metricName, metricRegistry, timeout)
      } else {
        new NJHealthCheck[F] {
          override def register(hc: F[Boolean]): Resource[F, Unit] =
            Resource.unit[F]
          override def register(hc: F[Boolean], policy: Policy, zoneId: ZoneId): Resource[F, Unit] =
            Resource.unit[F]
        }
      }
  }
}
