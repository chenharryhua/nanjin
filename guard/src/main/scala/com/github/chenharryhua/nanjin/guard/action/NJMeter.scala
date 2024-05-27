package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.{catsSyntaxHash, toFunctorOps}
import com.codahale.metrics.{Counter, Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.{CounterKind, MeterKind}
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, NJUnits}

final class NJMeter[F[_]: Sync] private (
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val isCounting: Boolean,
  private[this] val unit: MeasurementUnit) {

  private[this] val F = Sync[F]

  private[this] val meter_name: String =
    MetricID(name, Category.Meter(MeterKind.Meter, unit), token.hash).identifier

  private[this] val counter_name: String =
    MetricID(name, Category.Counter(CounterKind.Meter), token.hash).identifier

  private[this] lazy val meter: Meter     = metricRegistry.meter(meter_name)
  private[this] lazy val counter: Counter = metricRegistry.counter(counter_name)

  private[this] val calculate: Long => Unit =
    if (isCounting) { (num: Long) =>
      meter.mark(num)
      counter.inc(num)
    } else (num: Long) => meter.mark(num)

  def unsafeMark(num: Long): Unit = calculate(num)
  def mark(num: Long): F[Unit]    = F.delay(calculate(num))

  def kleisli[A](f: A => Long): Kleisli[F, A, Unit] = Kleisli(mark).local(f)

  private val unregister: F[Unit] = F.delay {
    metricRegistry.remove(meter_name)
    metricRegistry.remove(counter_name)
  }.void
}

object NJMeter {

  final class Builder private[guard] (measurement: Measurement, unit: MeasurementUnit, isCounting: Boolean) {
    def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), unit, isCounting)

    def counted: Builder = new Builder(measurement, unit, true)

    def withUnit(f: NJUnits.type => MeasurementUnit): Builder =
      new Builder(measurement, f(NJUnits), isCounting)

    private[guard] def build[F[_]: Sync](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams): Resource[F, NJMeter[F]] = {
      val metricName = MetricName(serviceParams, measurement, name)
      Resource.make(Sync[F].unique.map(new NJMeter[F](_, metricName, metricRegistry, isCounting, unit)))(
        _.unregister)
    }
  }
}
