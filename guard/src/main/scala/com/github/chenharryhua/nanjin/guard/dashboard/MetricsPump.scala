package com.github.chenharryhua.nanjin.guard.dashboard

import cats.effect.kernel.Async
import cats.syntax.applicative.catsSyntaxApplicativeId
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, TickedValue}
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

final private class MetricsPump(metricRegistry: MetricRegistry) {

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

  def pumping[F[_]: Async](wsb2: WebSocketBuilder2[F], zoneId: ZoneId, policy: Policy): F[Response[F]] = {
    val send: Stream[F, WebSocketFrame] =
      tickStream.tickImmediate(zoneId, _.fresh(policy))
        .map(tick => TickedValue(tick, meters ++ timers))
        .zipWithPrevious
        .map { case (pre, curr) =>
          pre match {
            case Some(previous) =>
              curr.map(value =>
                value.foldLeft(previous.value) { case (sum, (mid, count)) =>
                  sum.updatedWith(mid)(_.map(count - _))
                })
            case None => curr.map(_.view.mapValues(_ => 0L).toMap[MetricID, Long])
          }
        }.map { tv =>
          val series = tv.value.map { case (mid, count) =>
            Json.obj(
              "name" -> Json.fromString(s"${mid.metricLabel.label}(${mid.metricName.name})"),
              "point" -> Json.obj(
                "x" -> tv.tick.conclude.toEpochMilli.asJson,
                "y" -> Json.fromLong(count)
              )
            )
          }

          WebSocketFrame.Text(Json.obj("series" -> series.asJson).noSpaces)
        }

    val receive: Pipe[F, WebSocketFrame, Unit] = _.evalMap(_ => ().pure[F])

    wsb2.build(send, receive)
  }

}
