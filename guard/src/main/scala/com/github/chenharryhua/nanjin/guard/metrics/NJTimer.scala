package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.effect.std.UUIDGen
import cats.implicits.{catsSyntaxTuple2Semigroupal, toFunctorOps}
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.TimerKind

import java.time.Duration as JavaDuration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

sealed trait NJTimer[F[_]] extends KleisliLike[F, Long] {

  def update(jd: JavaDuration): F[Unit]
  def update(num: Long): F[Unit]

  final def update(fd: FiniteDuration): F[Unit] =
    update(fd.toNanos)

  final override def run(num: Long): F[Unit] = update(num)

}

object NJTimer {
  def dummy[F[_]](implicit F: Applicative[F]): NJTimer[F] =
    new NJTimer[F] {
      override def update(jd: JavaDuration): F[Unit] = F.unit
      override def update(num: Long): F[Unit]        = F.unit
    }

  private class Impl[F[_]: Sync](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: MetricRegistry,
    private[this] val reservoir: Option[Reservoir],
    private[this] val name: MetricName
  ) extends NJTimer[F] {

    private[this] val F = Sync[F]

    private[this] val timer_name: String =
      MetricID(label, name, Category.Timer(TimerKind.Timer)).identifier

    private[this] val supplier: MetricRegistry.MetricSupplier[Timer] = () =>
      reservoir match {
        case Some(value) => new Timer(value)
        case None        => new Timer(new ExponentiallyDecayingReservoir) // default reservoir
      }

    private[this] lazy val timer: Timer = metricRegistry.timer(timer_name, supplier)

    override def update(num: Long): F[Unit]        = F.delay(timer.update(num, TimeUnit.NANOSECONDS))
    override def update(jd: JavaDuration): F[Unit] = F.delay(timer.update(jd))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(timer_name)).void

  }

  val initial: Builder = new Builder(isEnabled = true, reservoir = None)

  final class Builder private[guard] (
    isEnabled: Boolean,
    reservoir: Option[Reservoir]
  ) extends EnableConfig[Builder] {

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, Some(reservoir))

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, reservoir)

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: MetricRegistry)(implicit
      F: Sync[F]): Resource[F, NJTimer[F]] =
      if (isEnabled) {
        Resource.make((F.monotonic, UUIDGen[F].randomUUID).mapN { case (ts, unique) =>
          new Impl[F](
            label = label,
            metricRegistry = metricRegistry,
            reservoir = reservoir,
            name = MetricName(name, ts, unique))
        })(_.unregister)
      } else
        Resource.pure(dummy[F])
  }
}
