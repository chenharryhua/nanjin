package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.MeasurementName
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID}
import io.circe.syntax.EncoderOps

final class NJCounter[F[_]] private[guard] (
  name: MeasurementName,
  metricRegistry: MetricRegistry,
  tag: Option[String])(implicit F: Sync[F]) {

  private lazy val counter: Counter =
    metricRegistry.counter(MetricID(name, MetricCategory.Counter(tag.getOrElse("counter"))).asJson.noSpaces)

  def withTag(tag: String): NJCounter[F] = new NJCounter[F](name, metricRegistry, Some(tag))

  def unsafeInc(num: Long): Unit = counter.inc(num)
  def unsafeDec(num: Long): Unit = counter.dec(num)

  def inc(num: Long): F[Unit] = F.delay(unsafeInc(num))
  def dec(num: Long): F[Unit] = F.delay(unsafeDec(num))
}
