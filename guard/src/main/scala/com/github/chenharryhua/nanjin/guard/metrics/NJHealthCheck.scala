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

object NJHealthCheck {
  def dummy[F[_]]: NJHealthCheck[F] =
    new NJHealthCheck[F] {
      override def register(hc: F[Boolean]): Resource[F, Unit] =
        Resource.unit[F]
      override def register(hc: F[Boolean], policy: Policy, zoneId: ZoneId): Resource[F, Unit] =
        Resource.unit[F]
    }

  private class Impl[F[_]: Async](
    private[this] val name: MetricName,
    private[this] val metricRegistry: MetricRegistry,
    private[this] val timeout: FiniteDuration,
    private[this] val tag: MetricTag)
      extends NJHealthCheck[F] {

    private[this] val F = Async[F]

    override def register(hc: F[Boolean]): Resource[F, Unit] =
      Dispatcher.sequential[F].flatMap { dispatcher =>
        Resource.eval(F.unique).flatMap { token =>
          val metricID: MetricID = MetricID(name, tag, Category.Gauge(GaugeKind.HealthCheck), token)
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

  final class Builder private[guard] (isEnabled: Boolean, timeout: FiniteDuration)
      extends EnableConfig[Builder] {

    def withTimeout(timeout: FiniteDuration): Builder =
      new Builder(isEnabled, timeout)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, timeout)

    private[guard] def build[F[_]: Async](
      metricName: MetricName,
      tag: MetricTag,
      metricRegistry: MetricRegistry): NJHealthCheck[F] =
      if (isEnabled) {
        new Impl[F](metricName, metricRegistry, timeout, tag)
      } else dummy[F]
  }
}
