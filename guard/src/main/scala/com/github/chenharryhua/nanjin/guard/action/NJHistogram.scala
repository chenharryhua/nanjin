package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Histogram, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Category, MetricID, MetricName}
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

final class NJHistogram[F[_]] private[guard] (
  name: MetricName,
  unit: StandardUnit,
  metricRegistry: MetricRegistry)(implicit F: Sync[F]) {
  private lazy val histogram: Histogram =
    metricRegistry.histogram(MetricID(name, Category.Histogram(unit)).asJson.noSpaces)

  def unsafeUpdate(num: Long): Unit = histogram.update(num)

  def update(num: Long): F[Unit] = F.delay(unsafeUpdate(num))
}
