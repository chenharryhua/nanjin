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

final class NJHealthCheck[F[_]: Async] private (
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val timeout: FiniteDuration) {

  private[this] val F = Async[F]

  def register(hc: F[Boolean]): Resource[F, Unit] =
    Dispatcher.sequential[F].flatMap { dispatcher =>
      Resource.eval(F.unique).flatMap { token =>
        val metricID: MetricID = MetricID(name, Category.Gauge(GaugeKind.HealthCheck), token.hash)
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
  def register(hc: => Boolean): Resource[F, Unit] = register(F.delay(hc))

  /** heath check sometimes is expensive.
    * @param hc
    *   health check method.
    */
  def register(hc: F[Boolean], policy: Policy, zoneId: ZoneId): Resource[F, Unit] = {
    val check: F[Boolean] = hc.timeout(timeout).attempt.map(_.fold(_ => false, identity))
    for {
      init <- Resource.eval(check)
      ref <- Resource.eval(F.ref(init))
      _ <- F.background(tickStream[F](policy, zoneId).evalMap(_ => check.flatMap(ref.set)).compile.drain)
      _ <- register(ref.get)
    } yield ()
  }

  def register(hc: => Boolean, policy: Policy, zoneId: ZoneId): Resource[F, Unit] =
    register(F.delay(hc), policy, zoneId)
}

object NJHealthCheck {

  final class Builder private[guard] (measurement: Measurement, timeout: FiniteDuration) {

    def withMeasurement(measurement: String): Builder = new Builder(Measurement(measurement), timeout)

    def withTimeout(timeout: FiniteDuration): Builder = new Builder(measurement, timeout)

    private[guard] def build[F[_]: Async](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams): NJHealthCheck[F] = {
      val metricName = MetricName(serviceParams, measurement, name)
      new NJHealthCheck[F](metricName, metricRegistry, timeout)
    }
  }
}
