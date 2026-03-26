package com.github.chenharryhua.nanjin.guard.service.dashboard

import com.github.chenharryhua.nanjin.guard.event.MetricID
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.websocket.WebSocketFrame

import java.time.Instant

final private case class TimedMeters(time: Instant, meters: Map[MetricID, Long]) {
  def text: WebSocketFrame.Text = {
    val series = meters.map { case (mid, count) =>
      Json.obj(
        "label" -> Json.fromString(s"${mid.metricLabel.label}(${mid.metricName.name})"),
        "value" -> Json.fromLong(count)
      )
    }
    WebSocketFrame.Text(
      Json.obj(
        "ts" -> Json.fromLong(time.toEpochMilli),
        "series" -> series.asJson
      ).noSpaces)
  }

  def merge(prev: TimedMeters): TimedMeters = {
    val nd = meters.foldLeft(Map.empty[MetricID, Long]) { case (sum, (mid, count)) =>
      prev.meters.get(mid) match
        case Some(value) => sum + (mid -> (count - value))
        case None        => sum + (mid -> 0)
    }
    TimedMeters(time, nd)
  }
}
