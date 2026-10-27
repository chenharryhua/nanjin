package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.effect.kernel.Async
import cats.effect.std.Queue
import cats.syntax.applicative.given
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.MeteredCounts
import com.github.chenharryhua.nanjin.guard.service.History
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
        "label" -> Json.fromString(s"${mid.scope.label}(${mid.token.metricName})"),
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
        // Register the topic subscription *before* snapshotting history, so a sample published in
        // the window between the two cannot fall into neither. `subscribeAwait` acquires the
        // subscriber eagerly on Resource acquisition (unlike `subscribe`, which registers lazily
        // when the stream is first pulled). A sample straddling the boundary may appear in both the
        // history snapshot and the live stream; a duplicated chart point is harmless, a gap is not.
        //
        // Per-client isolation: the shared topic has a single publisher fanning out via `publish1`,
        // which semantically blocks whenever ANY subscriber's `subscribeAwait` queue is full. If the
        // live stream fed the socket directly, a slow or stalled client would let its queue fill and
        // stall the publisher — freezing the feed for every other client. So we drain the subscriber
        // (history snapshot ++ live) into a per-client `circularBuffer`, whose `offer` never blocks
        // (it drops the oldest point when full). That drainer keeps the `subscribeAwait` queue empty
        // regardless of socket speed, so one laggard can never back-pressure the shared publisher; a
        // slow client simply loses the oldest points, which is the right trade-off for a live chart.
        val send: Stream[F, WebSocketFrame] =
          Stream.resource(topic.subscribeAwait(5)).flatMap { live =>
            Stream.eval(Queue.circularBuffer[F, MeteredCounts](backendConfig.maxPoints.value)).flatMap {
              buffer =>
                val drain: Stream[F, Nothing] =
                  (Stream.evalSeq(history.value) ++ live).foreach(buffer.offer).drain
                Stream.fromQueueUnterminated(buffer).map(text).concurrently(drain)
            }
          }

        val receive: Pipe[F, WebSocketFrame, Unit] = _.evalMap(_ => ().pure[F])

        wsb2.build(send, receive)

      case GET -> Root / file =>
        StaticFile.fromResource(s"dashboard/$file").getOrElseF(NotFound())
    }
}
