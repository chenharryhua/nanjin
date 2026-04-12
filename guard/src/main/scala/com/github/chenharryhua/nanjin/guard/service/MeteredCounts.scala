package com.github.chenharryhua.nanjin.guard.service

import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import com.github.chenharryhua.nanjin.guard.event.MetricID

import java.time.Instant

opaque type MeteredCounts = TickedValue[Map[MetricID, Long]]

object MeteredCounts {
  def apply(tick: Tick, value: Map[MetricID, Long]): MeteredCounts = TickedValue(tick, value)

  extension (mc: MeteredCounts)
    def timestamp: Instant = mc.tick.conclude
    def counts: Map[MetricID, Long] = mc.value

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
