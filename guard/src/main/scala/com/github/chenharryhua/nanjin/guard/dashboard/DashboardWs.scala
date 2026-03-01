package com.github.chenharryhua.nanjin.guard.dashboard

import cats.data.Kleisli
import cats.effect.kernel.Async
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, TickedValue}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*
import org.http4s.server.Router
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.websocket.WebSocketFrame
import org.http4s.{HttpRoutes, Request, Response, StaticFile}
import scalatags.Text
import scalatags.Text.all.*

import java.time.ZoneId

final class DashboardWs[F[_]](
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  zoneId: ZoneId,
  policy: Policy)(implicit F: Async[F])
    extends Http4sDsl[F] {

  private val fetch = new FetchCounters(metricRegistry)

  private val html_page: Text.TypedTag[String] =
    html(
      head(
        tag("title")(serviceParams.serviceName.value),
        script(src := "https://cdn.jsdelivr.net/npm/chart.js"),
        script(src := "https://cdn.jsdelivr.net/npm/luxon"),
        script(src := "https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon")
      ),
      body(
        div(
          id                := "chart-root",
          data("ws-port")   := serviceParams.host.port.map(_.value).getOrElse(1026),
          data("zone-id")   := zoneId.toString
        ),
        script(`type` := "module", src := "/dashboard/frontend.js")
      )
    )

  private def metrics(wsb2: WebSocketBuilder2[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "dashboard" / file =>
      StaticFile.fromResource(s"dashboard/$file").getOrElseF(NotFound())

    case GET -> Root / "dashboard" => Ok(html_page)

    case GET -> Root / "ws" =>
      val send: Stream[F, WebSocketFrame] =
        tickStream.tickScheduled(zoneId, _.fresh(policy))
          .map(tick => TickedValue(tick, fetch.meters.value))
          .zipWithPrevious
          .map { case (pre, curr) =>
            pre match {
              case Some(previous) =>
                curr.map(value =>
                  value.foldLeft(previous.value) { case (sum, (mid, count)) =>
                    sum.updatedWith(mid)(_.map(count - _))
                  })
              case None => curr.map(_.view.mapValues(_ => 0L).toMap)
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

      val receive: Pipe[F, WebSocketFrame, Unit] = _.evalMap(_ => F.unit)

      wsb2.build(send, receive)

    case GET -> Root / "report" => Ok("good")
  }

  def dashboardRouter(wsb2: WebSocketBuilder2[F]): Kleisli[F, Request[F], Response[F]] =
    Router("/" -> metrics(wsb2)).orNotFound
}
