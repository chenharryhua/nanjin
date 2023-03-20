package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Histogram, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.MeasurementName
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID}
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

final class NJHistogram[F[_]] private[guard] (
  name: MeasurementName,
  unit: StandardUnit,
  metricRegistry: MetricRegistry,
  tag: Option[String])(implicit F: Sync[F]) {
  private lazy val histogram: Histogram =
    metricRegistry.histogram(
      MetricID(name, MetricCategory.Histogram(unit, tag.getOrElse("histogram"))).asJson.noSpaces)

  def withTag(tag: String): NJHistogram[F] = new NJHistogram[F](name, unit, metricRegistry, Some(tag))

  def unsafeUpdate(num: Long): Unit =
    histogram.update(num)

  def update(num: Long): F[Unit] = F.delay(unsafeUpdate(num))
}
