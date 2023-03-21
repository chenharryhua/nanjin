package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Meter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Category, MetricID, MetricName}
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

// counter can be reset, meter can't
final class NJMeter[F[_]] private[guard] (
  name: MetricName,
  metricRegistry: MetricRegistry,
  unit: StandardUnit)(implicit F: Sync[F]) {

  private lazy val meter: Meter =
    metricRegistry.meter(MetricID(name, Category.Meter(unit)).asJson.noSpaces)

  def unsafeMark(num: Long): Unit = meter.mark(num)

  def mark(num: Long): F[Unit] = F.delay(unsafeMark(num))

}
