package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Sync
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.MeasurementID
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID}
import io.circe.syntax.EncoderOps

final class NJCounter[F[_]] private[guard] (id: MeasurementID, metricRegistry: MetricRegistry)(implicit
  F: Sync[F]) {

  private lazy val counter: Counter =
    metricRegistry.counter(MetricID(id, MetricCategory.Counter).asJson.noSpaces)

  def unsafeInc(num: Long): Unit = counter.inc(num)
  def unsafeDec(num: Long): Unit = counter.dec(num)

  def inc(num: Long): F[Unit] = F.delay(unsafeInc(num))
  def dec(num: Long): F[Unit] = F.delay(unsafeDec(num))

}
