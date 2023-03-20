package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.MeasurementName
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID}
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

// counter can be reset, meter can't
final class NJMeter[F[_]] private[guard] (
  name: MeasurementName,
  metricRegistry: MetricRegistry,
  unit: StandardUnit,
  tag: Option[String])(implicit F: Sync[F]) {

  private lazy val meter: Meter =
    metricRegistry.meter(MetricID(name, MetricCategory.Meter(unit, tag.getOrElse("meter"))).asJson.noSpaces)

  def withTag(tag: String): NJMeter[F] = new NJMeter[F](name, metricRegistry, unit, Some(tag))

  def unsafeMark(num: Long): Unit = meter.mark(num)

  def mark(num: Long): F[Unit] = F.delay(unsafeMark(num))

}
