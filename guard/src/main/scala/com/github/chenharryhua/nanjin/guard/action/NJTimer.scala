package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.{catsSyntaxHash, toFunctorOps}
import com.codahale.metrics.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.{CounterKind, TimerKind}

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

final class NJTimer[F[_]: Sync] private (
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val isCounting: Boolean,
  private[this] val reservoir: Option[Reservoir]) {

  private[this] val F = Sync[F]

  private[this] val timer_name: String =
    MetricID(name, Category.Timer(TimerKind.Timer), token.hash).identifier

  private[this] val counter_name: String =
    MetricID(name, Category.Counter(CounterKind.Timer), token.hash).identifier

  private[this] val supplier: MetricRegistry.MetricSupplier[Timer] = () =>
    reservoir match {
      case Some(value) => new Timer(value)
      case None        => new Timer(new ExponentiallyDecayingReservoir) // default reservoir
    }

  private[this] lazy val timer: Timer     = metricRegistry.timer(timer_name, supplier)
  private[this] lazy val counter: Counter = metricRegistry.counter(counter_name)

  private[this] val calculate: Duration => Unit =
    if (isCounting) { (duration: Duration) =>
      timer.update(duration)
      counter.inc(1)
    } else (duration: Duration) => timer.update(duration)

  def update(fd: FiniteDuration): F[Unit] = F.delay(calculate(fd.toJava))

  def unsafeUpdate(fd: FiniteDuration): Unit = calculate(fd.toJava)

  def kleisli[A](f: A => FiniteDuration): Kleisli[F, A, Unit] = Kleisli(update).local(f)

  private val unregister: F[Unit] = F.delay {
    metricRegistry.remove(timer_name)
    metricRegistry.remove(counter_name)
  }.void
}

object NJTimer {

  final class Builder private[guard] (
    measurement: Measurement,
    isCounting: Boolean,
    reservoir: Option[Reservoir]) {

    def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), isCounting, reservoir)

    def counted: Builder = new Builder(measurement, true, reservoir)

    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(measurement, isCounting, Some(reservoir))

    private[guard] def build[F[_]: Sync](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams): Resource[F, NJTimer[F]] = {
      val metricName = MetricName(serviceParams, measurement, name)
      Resource.make(Sync[F].unique.map(new NJTimer[F](_, metricName, metricRegistry, isCounting, reservoir)))(
        _.unregister)
    }
  }
}
