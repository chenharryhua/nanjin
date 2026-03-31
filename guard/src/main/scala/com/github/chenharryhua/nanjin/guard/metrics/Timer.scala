package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.functor.given
import com.codahale.metrics.{
  ExponentiallyDecayingReservoir,
  MetricRegistry,
  Reservoir,
  Timer as CodahaleTimer
}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.event.{Category, MetricID, MetricLabel, MetricName, TimerKind}

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
  private class Impl[F[_]](
    label: MetricLabel,
    metricRegistry: MetricRegistry,
    reservoir: Option[Reservoir],
    name: MetricName
  )(implicit F: Sync[F])
      extends Timer[F] {

    private val timer_name: String =
      MetricID(label, name, Category.Timer(TimerKind.Timer)).identifier

    private val supplier: MetricRegistry.MetricSupplier[CodahaleTimer] = () =>
      reservoir match {
        case Some(value) => new CodahaleTimer(value)
        case None        => new CodahaleTimer(new ExponentiallyDecayingReservoir) // default reservoir
      }

    private lazy val timer: CodahaleTimer = metricRegistry.timer(timer_name, supplier)

    override def elapsedNano(num: Long): F[Unit] = F.delay(timer.update(num, TimeUnit.NANOSECONDS))

    override def timing[A](fa: F[A]): F[A] = F.map(F.timed(fa)) { case (fd, result) =>
      timer.update(fd.toNanos, TimeUnit.NANOSECONDS)
      result
    }

    val unregister: F[Unit] = F.delay(metricRegistry.remove(timer_name)).void

  }

  final class Builder private[Timer] (
    isEnabled: Boolean,
    reservoir: Option[Reservoir]
  ) extends EnableConfig[Builder] {

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, Some(reservoir))

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, reservoir)

    private[Timer] def build[F[_]](label: MetricLabel, name: String, metricRegistry: MetricRegistry)(using
      F: Sync[F]): Resource[F, Timer[F]] = {
      val timer: Resource[F, Timer[F]] =
        Resource.make(MetricName(name).map(Impl[F](label, metricRegistry, reservoir, _)))(_.unregister)

      val noop: Timer[F] =
        new Timer[F] {
          override def elapsedNano(num: Long): F[Unit] = F.unit
          override def timing[A](fa: F[A]): F[A] = fa
        }

      if isEnabled then timer else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    label: MetricLabel,
    name: String,
    f: Endo[Builder]): Resource[F, Timer[F]] =
    f(new Builder(isEnabled = true, reservoir = None))
      .build[F](label, name, mr)
}
