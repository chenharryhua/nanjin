package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.chrono.TickedValue
import com.github.chenharryhua.nanjin.guard.event.MetricID
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.websocket.WebSocketFrame

private opaque type TickedMeters = TickedValue[Map[MetricID, Long]]

private object TickedMeters {
  def apply(tv: TickedValue[Map[MetricID, Long]]): TickedMeters = tv

  extension (tm: TickedMeters)
    def text: WebSocketFrame.Text = {
      val series = tm.value.map { case (mid, count) =>
        Json.obj(
          "label" -> Json.fromString(s"${mid.metricLabel.label}(${mid.metricName.name})"),
          "value" -> Json.fromLong(count)
        )
      }
      WebSocketFrame.Text(
        Json.obj(
          "ts" -> Json.fromLong(tm.tick.conclude.toEpochMilli),
          "series" -> series.asJson
        ).noSpaces)
    }

    def merge(prev: TickedMeters): TickedMeters = {
      val nd = tm.value.foldLeft(Map.empty[MetricID, Long]) { case (sum, (mid, count)) =>
        prev.value.get(mid) match
          case Some(value) => sum + (mid -> (count - value))
          case None        => sum + (mid -> count)
      }
      tm.as(nd)
    }
}
