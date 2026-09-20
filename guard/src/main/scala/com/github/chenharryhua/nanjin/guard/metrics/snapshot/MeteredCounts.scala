package com.github.chenharryhua.nanjin.guard.metrics.snapshot

import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import com.github.chenharryhua.nanjin.guard.metrics.{MetricScope, MetricToken, Squants}

import java.time.Instant

final case class MeteredId(scope: MetricScope, token: MetricToken, squants: Squants)

opaque type MeteredCounts = TickedValue[Map[MeteredId, Long]]

object MeteredCounts {
  def apply(tick: Tick, value: Map[MeteredId, Long]): MeteredCounts = TickedValue(tick, value)

  extension (mc: MeteredCounts)
    def timestamp: Instant = mc.tick.conclude
    def counts: Map[MeteredId, Long] = mc.value

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
