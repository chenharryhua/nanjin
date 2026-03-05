package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.effect.kernel.{Async, Ref}
import cats.syntax.applicative.catsSyntaxApplicativeId
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.SingleFlight
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickedValue}
import com.github.chenharryhua.nanjin.guard.event.MetricID
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.http4s.Response
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.websocket.WebSocketFrame

import java.time.ZoneId
import scala.jdk.CollectionConverters.MapHasAsScala

final private class MetricsPump[F[_]: Async](
  metricRegistry: MetricRegistry,
  zoneId: ZoneId,
  policy: Policy,
  ref: Ref[F, TickedValue[Map[MetricID, Long]]],
  singleFlight: SingleFlight[F, TickedValue[Map[MetricID, Long]]]) {

  private def meters: Map[MetricID, Long] =
    metricRegistry.getMeters().asScala
      .flatMap { case (mid, meter) =>
        decode[MetricID](mid).toOption.map(_ -> meter.getCount)
      }.toMap

  private def timers: Map[MetricID, Long] =
    metricRegistry.getTimers().asScala
      .flatMap { case (mid, timer) =>
        decode[MetricID](mid).toOption.map(_ -> timer.getCount)
      }.toMap

  private def retrieve(tick: Tick): F[TickedValue[Map[MetricID, Long]]] =
    singleFlight {
      ref.modify { tv =>
        if (tv.tick.isWithinClosedOpen(tick.acquires))
          (tv, tv)
        else {
          val update = TickedValue(tick, meters ++ timers)
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
        "series" -> series.asJson,
        "ts" -> Json.fromLong(tv.tick.commence.toEpochMilli)
      ).noSpaces)
  }

  def pumping(wsb2: WebSocketBuilder2[F]): F[Response[F]] = {
    val send: Stream[F, WebSocketFrame] =
      tickStream.tickFuture(zoneId, _.fresh(policy))
        .evalMap(retrieve)
        .zipWithPrevious
        .map { case (pre, curr) =>
          pre match {
            case Some(previous) =>
              curr.map(current =>
                current.foldLeft(Map.empty[MetricID, Long]) { case (sum, (mid, count)) =>
                  previous.value.get(mid) match {
                    case Some(value) => sum + (mid -> (count - value))
                    case None        => sum + (mid -> 0)
                  }
                })
            case None => curr.map(_.view.mapValues(_ => 0L).toMap[MetricID, Long])
          }
        }
        .map(interpret)

    val receive: Pipe[F, WebSocketFrame, Unit] = _.evalMap(_ => ().pure[F])

    wsb2.build(send, receive)
  }
}
