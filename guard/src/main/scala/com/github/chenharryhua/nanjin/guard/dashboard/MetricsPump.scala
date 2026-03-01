package com.github.chenharryhua.nanjin.guard.dashboard

import cats.Eval
import cats.effect.kernel.Async
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
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

  private val meters: Eval[Map[MetricID, Long]] =
    Eval.always {
      metricRegistry.getMeters().asScala
        .foldLeft(Map.empty[MetricID, Long]) { case (map, (name, meter)) =>
          decode[MetricID](name) match {
            case Left(_)    => map
            case Right(mid) => map + (mid -> meter.getCount)
          }
        }
    }

  private val timers: Eval[Map[MetricID, Long]] =
    Eval.always {
      metricRegistry.getTimers().asScala
        .foldLeft(Map.empty[MetricID, Long]) { case (map, (name, timer)) =>
          decode[MetricID](name) match {
            case Left(_)    => map
            case Right(mid) => map + (mid -> timer.getCount)
          }
        }
    }

  def pumping[F[_]: Async](wsb2: WebSocketBuilder2[F], zoneId: ZoneId, policy: Policy): F[Response[F]] = {
    val send: Stream[F, WebSocketFrame] =
      tickStream.tickImmediate(zoneId, _.fresh(policy))
        .map(tick => TickedValue(tick, (meters, timers).mapN(_ ++ _).value))
        .zipWithPrevious
        .map { case (pre, curr) =>
          pre match {
            case Some(previous) =>
              curr.map(value =>
                value.foldLeft(previous.value) { case (sum, (mid, count)) =>
                  sum.updatedWith(mid)(_.map(count - _))
                })
            case None => curr.map(_ => Map.empty[MetricID, Long])
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
