package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.effect.implicits.clockOps
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.toFunctorOps
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.TimerKind

import java.time.Duration as JavaDuration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

sealed trait NJTimer[F[_]] extends KleisliLike[F, Long] {
  def run(num: Long): F[Unit]
  def update(jd: JavaDuration): F[Unit]
  def update(num: Long): F[Unit]

  def timing[A](fa: F[A]): F[A]

  final def update(fd: FiniteDuration): F[Unit] =
    update(fd.toJava)

}

object NJTimer {
  def dummy[F[_]](implicit F: Applicative[F]): NJTimer[F] =
    new NJTimer[F] {
      override def timing[A](fa: F[A]): F[A]         = fa
      override def update(jd: JavaDuration): F[Unit] = F.unit
      override def update(num: Long): F[Unit]        = F.unit
      override def run(num: Long): F[Unit]           = F.unit
    }

  private class Impl[F[_]: Sync](
    private[this] val token: FiniteDuration,
    private[this] val name: MetricName,
    private[this] val metricRegistry: MetricRegistry,
    private[this] val reservoir: Option[Reservoir],
    private[this] val tag: MetricTag
  ) extends NJTimer[F] {

    private[this] val F = Sync[F]

    private[this] val timer_name: String =
      MetricID(name, tag, Category.Timer(TimerKind.Timer), token).identifier

    private[this] val supplier: MetricRegistry.MetricSupplier[Timer] = () =>
      reservoir match {
        case Some(value) => new Timer(value)
        case None        => new Timer(new ExponentiallyDecayingReservoir) // default reservoir
      }

    private[this] lazy val timer: Timer = metricRegistry.timer(timer_name, supplier)

    override def timing[A](fa: F[A]): F[A] =
      fa.timed.map { case (fd, result) =>
        timer.update(fd.toJava)
        result
      }

    override def run(num: Long): F[Unit]           = F.delay(timer.update(num, TimeUnit.NANOSECONDS))
    override def update(num: Long): F[Unit]        = F.delay(timer.update(num, TimeUnit.NANOSECONDS))
    override def update(jd: JavaDuration): F[Unit] = F.delay(timer.update(jd))

    val unregister: F[Unit] = F.delay(metricRegistry.remove(timer_name)).void

  }

  final class Builder private[guard] (
    isEnabled: Boolean,
    reservoir: Option[Reservoir]
  ) extends EnableConfig[Builder] {

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, Some(reservoir))

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, reservoir)

    private[guard] def build[F[_]](metricName: MetricName, tag: MetricTag, metricRegistry: MetricRegistry)(
      implicit F: Sync[F]): Resource[F, NJTimer[F]] =
      if (isEnabled) {
        Resource.make(
          F.monotonic.map(token =>
            new Impl[F](
              token = token,
              name = metricName,
              metricRegistry = metricRegistry,
              reservoir = reservoir,
              tag = tag)))(_.unregister)
      } else
        Resource.pure(dummy[F])
  }
}
