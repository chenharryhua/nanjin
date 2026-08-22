package com.github.chenharryhua.nanjin.guard.metrics.snapshot

import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import com.github.chenharryhua.nanjin.guard.metrics.{MetricLabel, MetricName, Squants}

import java.time.Instant

final case class MeteredID(metricLabel: MetricLabel, metricName: MetricName, squants: Squants)

opaque type MeteredCounts = TickedValue[Map[MeteredID, Long]]

object MeteredCounts {
  def apply(tick: Tick, value: Map[MeteredID, Long]): MeteredCounts = TickedValue(tick, value)

  extension (mc: MeteredCounts)
    def timestamp: Instant = mc.tick.conclude
    def counts: Map[MeteredID, Long] = mc.value

    def delta(prev: MeteredCounts): MeteredCounts = {
      val prevMap = prev.value
      val nd = mc.value.iterator.map { case (mid, count) =>
        val diff = prevMap.get(mid) match
          case Some(prevCount) => count - prevCount
          case None            => count
        mid -> diff
      }.toMap

      mc.as(nd)
    }
}
