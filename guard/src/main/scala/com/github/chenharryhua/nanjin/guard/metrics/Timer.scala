package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.{utils, EnableConfig}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.TimerKind

import java.time.Duration as JavaDuration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

trait Timer[F[_]] extends KleisliLike[F, Long] {

  def elapsed(jd: JavaDuration): F[Unit]
  def elapsed(num: Long): F[Unit]

  def timing[A](fa: F[A]): F[A]

  final def elapsed(fd: FiniteDuration): F[Unit] =
    elapsed(fd.toNanos)

  final override def run(num: Long): F[Unit] = elapsed(num)

}

object Timer {
  def noop[F[_]](implicit F: Applicative[F]): Timer[F] =
    new Timer[F] {
      override def elapsed(jd: JavaDuration): F[Unit] = F.unit
      override def elapsed(num: Long): F[Unit] = F.unit

      override def timing[A](fa: F[A]): F[A] = fa
    }

  private class Impl[F[_]: Sync](
    private[this] val label: MetricLabel,
    private[this] val metricRegistry: metrics.MetricRegistry,
    private[this] val reservoir: Option[metrics.Reservoir],
    private[this] val name: MetricName
  ) extends Timer[F] {

    private[this] val F = Sync[F]

    private[this] val timer_name: String =
      MetricID(label, name, Category.Timer(TimerKind.Timer)).identifier

    private[this] val supplier: metrics.MetricRegistry.MetricSupplier[metrics.Timer] = () =>
      reservoir match {
        case Some(value) => new metrics.Timer(value)
        case None        => new metrics.Timer(new metrics.ExponentiallyDecayingReservoir) // default reservoir
      }

    private[this] lazy val timer: metrics.Timer = metricRegistry.timer(timer_name, supplier)

    override def elapsed(num: Long): F[Unit] = F.delay(timer.update(num, TimeUnit.NANOSECONDS))
    override def elapsed(jd: JavaDuration): F[Unit] = F.delay(timer.update(jd))

    override def timing[A](fa: F[A]): F[A] = F.map(F.timed(fa)) { case (fd, result) =>
      timer.update(fd.toNanos, TimeUnit.NANOSECONDS)
      result
    }

    val unregister: F[Unit] = F.delay(metricRegistry.remove(timer_name)).void

  }

  final class Builder private[guard] (
    isEnabled: Boolean,
    reservoir: Option[metrics.Reservoir]
  ) extends EnableConfig[Builder] {

    def withReservoir(reservoir: metrics.Reservoir): Builder =
      new Builder(isEnabled, Some(reservoir))

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, reservoir)

    private[guard] def build[F[_]](label: MetricLabel, name: String, metricRegistry: metrics.MetricRegistry)(
      implicit F: Sync[F]): Resource[F, Timer[F]] = {
      val timer: Resource[F, Timer[F]] =
        Resource.make((F.monotonic, utils.randomUUID[F]).mapN { case (ts, unique) =>
          new Impl[F](
            label = label,
            metricRegistry = metricRegistry,
            reservoir = reservoir,
            name = MetricName(name, ts, unique))
        })(_.unregister)

      fold_create_noop(isEnabled)(timer, Resource.pure(noop[F]))
    }
  }
}
