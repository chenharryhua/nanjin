package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.MeasurementID
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID}
import io.circe.syntax.EncoderOps

// counter can be reset, meter can't
final class NJMeter[F[_]] private[guard] (
  id: MeasurementID,
  metricRegistry: MetricRegistry,
  isCounting: Boolean)(implicit F: Sync[F]) {

  private lazy val meter: Meter =
    metricRegistry.meter(MetricID(id, MetricCategory.Meter).asJson.noSpaces)
  private lazy val counter: Counter =
    metricRegistry.counter(MetricID(id, MetricCategory.MeterCounter).asJson.noSpaces)

  def withCounting: NJMeter[F] = new NJMeter[F](id, metricRegistry, true)

  def unsafeMark(num: Long): Unit = {
    meter.mark(num)
    if (isCounting) counter.inc(num)
  }

  def mark(num: Long): F[Unit] = F.delay(unsafeMark(num))

}
