package com.github.chenharryhua.nanjin.guard.metrics

import cats.effect.implicits.genTemporalOps
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Dispatcher, UUIDGen}
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.GaugeKind

import java.time.ZoneId
import scala.concurrent.duration.{DurationInt, FiniteDuration}
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
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: MetricRegistry,
    private[this] val timeout: FiniteDuration,
    private[this] val name: String)
      extends NJHealthCheck[F] {

    private[this] val F = Async[F]

    override def register(hc: F[Boolean]): Resource[F, Unit] =
      for {
        dispatcher <- Dispatcher.sequential[F]
        metricID <- Resource.eval((F.monotonic, UUIDGen[F].randomUUID).mapN { case (ts, unique) =>
          MetricID(label, MetricName(name, ts, unique), Category.Gauge(GaugeKind.HealthCheck)).identifier
        })
        _ <- Resource.make(
          F.delay(
            metricRegistry.gauge(
              metricID,
              () =>
                new Gauge[Boolean] {
                  override def getValue: Boolean =
                    Try(dispatcher.unsafeRunTimed(hc, timeout)).fold(_ => false, identity)
                }
            )))(_ => F.delay(metricRegistry.remove(metricID)).void)
      } yield ()

    override def register(hc: F[Boolean], policy: Policy, zoneId: ZoneId): Resource[F, Unit] = {
      val check: F[Boolean] = hc.timeout(timeout).attempt.map(_.fold(_ => false, identity))
      for {
        init <- Resource.eval(check)
        ref <- Resource.eval(F.ref(init))
        _ <- F.background(
          tickStream.fromOne[F](policy, zoneId).evalMap(_ => check.flatMap(ref.set)).compile.drain)
        _ <- register(ref.get)
      } yield ()
    }
  }

  val initial: Builder = new Builder(isEnabled = true, timeout = 5.seconds)

  final class Builder private[guard] (isEnabled: Boolean, timeout: FiniteDuration)
      extends EnableConfig[Builder] {

    def withTimeout(timeout: FiniteDuration): Builder =
      new Builder(isEnabled, timeout)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, timeout)

    private[guard] def build[F[_]: Async](
      label: MetricLabel,
      name: String,
      metricRegistry: MetricRegistry): NJHealthCheck[F] =
      if (isEnabled) {
        new Impl[F](label, metricRegistry, timeout, name)
      } else dummy[F]
  }
}
