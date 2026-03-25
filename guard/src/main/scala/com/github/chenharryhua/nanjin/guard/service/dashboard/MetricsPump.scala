package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.effect.kernel.{Async, Ref}
import cats.syntax.applicative.catsSyntaxApplicativeId
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickedValue}
import com.github.chenharryhua.nanjin.common.resilience.SingleFlight
import com.github.chenharryhua.nanjin.guard.event.MetricID
import com.github.chenharryhua.nanjin.guard.service.ScrapeMetrics
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.Response
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.websocket.WebSocketFrame

import java.time.ZoneId

final private class MetricsPump[F[_]: Async](
  scrapeMetrics: ScrapeMetrics,
  zoneId: ZoneId,
  policy: Policy,
  ref: Ref[F, TickedValue[Map[MetricID, Long]]],
  singleFlight: SingleFlight[F, TickedValue[Map[MetricID, Long]]]) {

  private def retrieve(tick: Tick): F[TickedValue[Map[MetricID, Long]]] =
    singleFlight {
      ref.modify { tv =>
        if (tv.tick.isWithinClosedOpen(tick.acquires))
          (tv, tv)
        else {
          val update = TickedValue(tick, scrapeMetrics.meterCounters)
          (update, update)
        }
      }
    }

  private def interpret(tv: TickedValue[Map[MetricID, Long]]): WebSocketFrame.Text = {
    val series = tv.value.map { case (mid, count) =>
      Json.obj(
        "label" -> Json.fromString(s"${mid.metricLabel.label}(${mid.metricName.name})"),
        "value" -> Json.fromLong(count)
      )
    }
    WebSocketFrame.Text(
      Json.obj(
        "ts" -> Json.fromLong(tv.tick.commence.toEpochMilli),
        "series" -> series.asJson
      ).noSpaces)
  }

  def pumping(wsb2: WebSocketBuilder2[F]): F[Response[F]] = {
    val send: Stream[F, WebSocketFrame] =
      tickStream.tickFuture(zoneId, _.fresh(policy))
        .evalMap(retrieve)
        .zipWithPrevious
        .map {
          case (Some(prev), curr) =>
            curr.map {
              _.foldLeft(Map.empty[MetricID, Long]) { case (sum, (mid, count)) =>
                prev.value.get(mid) match
                  case Some(value) => sum + (mid -> (count - value))
                  case None        => sum + (mid -> 0)
              }
            }

          case (None, curr) =>
            curr.map(_.view.mapValues(_ => 0L).toMap[MetricID, Long])
        }
        .map(interpret)

    val receive: Pipe[F, WebSocketFrame, Unit] = _.evalMap(_ => ().pure[F])

    wsb2.build(send, receive)
  }
}
