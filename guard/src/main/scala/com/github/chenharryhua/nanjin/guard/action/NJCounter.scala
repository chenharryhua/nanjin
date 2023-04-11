package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Category, CounterKind, MetricID, MetricName}
import io.circe.syntax.EncoderOps

final class NJCounter[F[_]] private[guard] (
  name: MetricName,
  metricRegistry: MetricRegistry,
  isRisk: Boolean)(implicit F: Sync[F]) {

  private lazy val counter: Counter =
    if (isRisk)
      metricRegistry.counter(MetricID(name, Category.Counter(Some(CounterKind.RiskCounter))).asJson.noSpaces)
    else
      metricRegistry.counter(MetricID(name, Category.Counter(None)).asJson.noSpaces)

  def asRisk: NJCounter[F] = new NJCounter[F](name, metricRegistry, isRisk = true)

  def unsafeInc(num: Long): Unit = counter.inc(num)
  def unsafeDec(num: Long): Unit = counter.dec(num)

  def inc(num: Long): F[Unit] = F.delay(unsafeInc(num))
  def dec(num: Long): F[Unit] = F.delay(unsafeDec(num))
  def getCount: F[Long]       = F.delay(counter.getCount)
}
