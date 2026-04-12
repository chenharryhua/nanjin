package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.effect.kernel.Async
import cats.syntax.applicative.given
import com.github.chenharryhua.nanjin.guard.service.{History, MeteredCounts}
import fs2.concurrent.Topic
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.syntax.given
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.websocket.WebSocketFrame
import org.http4s.{HttpRoutes, StaticFile}
import scalatags.Text
import scalatags.Text.all.*

final private class HttpWsRouter[F[_]: Async](
  backendConfig: BackendConfig,
  topic: Topic[F, MeteredCounts],
  history: History[F, MeteredCounts]
) extends Http4sDsl[F] {

  private val home_page: Text.TypedTag[String] =
    html(
      head(
        tag("title")(backendConfig.serviceName),
        script(src := "https://cdn.jsdelivr.net/npm/chart.js"),
        script(src := "https://cdn.jsdelivr.net/npm/luxon"),
        script(src := "https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon"),
        link(rel   := "icon", href := "/dashboard/favicon.ico", `type` := "image/x-icon"),
        backendConfig.config
      ),
      body(
        div(id        := "dashboard"), // mount point
        script(`type` := "module", src := "/dashboard/nj-frontend.js"))
    )

  private def text(mc: MeteredCounts): WebSocketFrame.Text = {
    val series = mc.counts.map { case (mid, count) =>
      Json.obj(
        "label" -> Json.fromString(s"${mid.metricLabel.label}(${mid.metricName.name})"),
        "value" -> Json.fromLong(count)
      )
    }
    WebSocketFrame.Text(
      Json.obj(
        "ts" -> Json.fromLong(mc.timestamp.toEpochMilli),
        "series" -> series.asJson
      ).noSpaces)
  }

  def router(wsb2: WebSocketBuilder2[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      /*
       * Dashboard and Websocket
       */

      case GET -> Root        => Ok(home_page)
      case GET -> Root / "ws" =>
        val preserved = Stream.eval(history.value).flatMap(Stream.emits)
        val send: Stream[F, WebSocketFrame] =
          (preserved ++ topic.subscribe(5))
            .zipWithPrevious
            .map {
              case (Some(prev), curr) => curr.delta(prev)
              case (None, curr)       => curr
            }
            .map(text)

        val receive: Pipe[F, WebSocketFrame, Unit] = _.evalMap(_ => ().pure[F])

        wsb2.build(send, receive)

      case GET -> Root / file =>
        StaticFile.fromResource(s"dashboard/$file").getOrElseF(NotFound())
    }
}
