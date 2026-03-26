package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.data.Kleisli
import cats.effect.kernel.{Async, Ref}
import cats.effect.std.AtomicCell
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.chrono.tickStream
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.service.{LifecyclePublisher, MetricsPublisher, ScrapeMetrics}
import fs2.Stream
import fs2.concurrent.Topic
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Router
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.{Request, Response}

import scala.collection.immutable.Vector

private[service] object HttpServer {

  private def wsRouter[F[_]: Async](
    backendConfig: BackendConfig,
    scrapeMetrics: ScrapeMetrics): F[(HttpWsRouter[F], Stream[F, Nothing])] = {

    val ts: Stream[F, TimedMeters] =
      tickStream.tickScheduled[F](backendConfig.zoneId, _.fresh(backendConfig.policy))
        .map(tick => TimedMeters(tick.conclude, scrapeMetrics.meterCounters))

    for {
      topic <- Topic[F, TimedMeters]
      history <- Ref[F].of(Vector.empty[TimedMeters])
      stream = ts.evalMap { tm =>
        history.update { v =>
          if (v.size >= backendConfig.maxPoints)
            v.tail :+ tm
          else v :+ tm
        } >> topic.publish1(tm)
      }.onFinalize(history.set(Vector.empty) <* topic.close).drain
    } yield (HttpWsRouter[F](backendConfig, topic, history), stream)
  }

  def apply[F[_]: Async](
    emberServerBuilder: Option[EmberServerBuilder[F]],
    metricsPublisher: MetricsPublisher[F],
    lifecyclePublisher: LifecyclePublisher[F],
    errorHistory: AtomicCell[F, CircularFifoQueue[ReportedEvent]]): Stream[F, Nothing] =
    val dataRouter = HttpDataRouter[F](metricsPublisher, lifecyclePublisher, errorHistory).router

    emberServerBuilder match {
      case None      => Stream.empty.covary[F]
      case Some(esb) =>
        metricsPublisher.serviceParams.servicePolicies.realtimeMetrics match {
          case None =>
            Stream.resource(esb.withHttpApp(dataRouter.orNotFound).build) >> Stream.never
          case Some(rm) =>
            val bc = BackendConfig(
              serviceName = metricsPublisher.serviceParams.serviceName.value,
              zoneId = metricsPublisher.serviceParams.zoneId,
              port = esb.port,
              maxPoints = rm.maxPoints,
              policy = rm.policy
            )
            Stream.eval(wsRouter(bc, metricsPublisher.scrapeMetrics)).flatMap { case (ws, updates) =>
              def route(wsb2: WebSocketBuilder2[F]): Kleisli[F, Request[F], Response[F]] =
                Router(
                  "/" -> dataRouter,
                  "/dashboard" -> ws.router(wsb2)
                ).orNotFound
              val wsApp = Stream.resource(esb.withHttpWebSocketApp(route).build) >> Stream.never
              wsApp.concurrently(updates)
            }
        }
    }
}
