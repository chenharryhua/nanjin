package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.event.CategoryKind.TimerKind
import com.github.chenharryhua.nanjin.guard.event.{Category, MetricID, MetricLabel, MetricName}

import java.time.Duration as JavaDuration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

trait ToNanos[A]:
  def apply(a: A): Long
object ToNanos:
  given ToNanos[FiniteDuration] = _.toNanos
  given ToNanos[JavaDuration] = _.toNanos
end ToNanos

trait Timer[F[_]]:
  def elapsedNano(num: Long): F[Unit]
  def timing[A](fa: F[A]): F[A]

  final def elapsedNano(num: Int): F[Unit] =
    elapsedNano(num.toLong)
  final def elapsed[A: ToNanos](nano: A): F[Unit] =
    elapsedNano(summon[ToNanos[A]](nano))
end Timer

object Timer {
  def noop[F[_]](using F: Applicative[F]): Timer[F] =
    new Timer[F] {
      override def elapsedNano(num: Long): F[Unit] = F.unit
      override def timing[A](fa: F[A]): F[A] = fa
    }

  private class Impl[F[_]: Sync](
    private val label: MetricLabel,
    private val metricRegistry: metrics.MetricRegistry,
    private val reservoir: Option[metrics.Reservoir],
    private val name: MetricName
  ) extends Timer[F] {

    private val F = Sync[F]

    private val timer_name: String =
      MetricID(label, name, Category.Timer(TimerKind.Timer)).identifier

    private val supplier: metrics.MetricRegistry.MetricSupplier[metrics.Timer] = () =>
      reservoir match {
        case Some(value) => new metrics.Timer(value)
        case None        => new metrics.Timer(new metrics.ExponentiallyDecayingReservoir) // default reservoir
      }

    private lazy val timer: metrics.Timer = metricRegistry.timer(timer_name, supplier)

    override def elapsedNano(num: Long): F[Unit] = F.delay(timer.update(num, TimeUnit.NANOSECONDS))

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
      using F: Sync[F]): Resource[F, Timer[F]] = {
      val timer: Resource[F, Timer[F]] =
        Resource.make(MetricName(name).map { metricName =>
          new Impl[F](
            label = label,
            metricRegistry = metricRegistry,
            reservoir = reservoir,
            name = metricName)
        })(_.unregister)

      fold_create_noop(isEnabled)(timer, Resource.pure(noop[F]))
    }
  }
}
