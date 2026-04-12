package com.github.chenharryhua.nanjin.guard.service

import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import com.github.chenharryhua.nanjin.guard.event.MetricID
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.websocket.WebSocketFrame

opaque type MeteredCounts = TickedValue[Map[MetricID, Long]]

object MeteredCounts {
  def apply(tick: Tick, value: Map[MetricID, Long]): MeteredCounts = TickedValue(tick, value)

  extension (mc: MeteredCounts)
    def text: WebSocketFrame.Text = {
      val series = mc.value.map { case (mid, count) =>
        Json.obj(
          "label" -> Json.fromString(s"${mid.metricLabel.label}(${mid.metricName.name})"),
          "value" -> Json.fromLong(count)
        )
      }
      WebSocketFrame.Text(
        Json.obj(
          "ts" -> Json.fromLong(mc.tick.conclude.toEpochMilli),
          "series" -> series.asJson
        ).noSpaces)
    }

    def merge(prev: MeteredCounts): MeteredCounts = {
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
