package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.data.Kleisli
import cats.effect.kernel.Async
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.guard.config.Capacity
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.MeteredCounts
import com.github.chenharryhua.nanjin.guard.service.{
  History,
  MetricsEventHandler,
  ReportedEventHandler,
  ServiceEventHandler
}
import fs2.Stream
import fs2.concurrent.Topic
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Router
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.{Request, Response}

private[service] object HttpServer {

  private def wsRouter[F[_]: Async](
    backendConfig: BackendConfig,
    meh: MetricsEventHandler[F]): F[(HttpWsRouter[F], Stream[F, Nothing])] =
    for {
      topic <- Topic[F, MeteredCounts]
      history <- History[F, MeteredCounts](Some(Capacity(backendConfig.maxPoints.value + 1)))
      // `updates` is the single publisher to `topic`; closing the topic in its own finalizer is safe
      // precisely because of that. `topic.close` does not unblock producers parked on a subscriber bound,
      // so a second publisher to this topic could be left blocked after close -- keep it single-publisher.
      updates = meh.meteredCounts(_.fresh(backendConfig.policy))
        .evalMap(tm => history.add(tm) >> topic.publish1(tm))
        .onFinalize(history.clear <* topic.close)
        .drain
    } yield (HttpWsRouter[F](backendConfig, topic, history), updates)

  /** Serve only the data routes (no WebSocket feed), running the ember server for the life of the stream. */
  private def dataOnly[F[_]: Async](
    esb: EmberServerBuilder[F],
    dataRouter: org.http4s.HttpRoutes[F]): Stream[F, Nothing] =
    Stream.resource(esb.withHttpApp(dataRouter.orNotFound).build) >> Stream.never

  def apply[F[_]: Async](
    emberServerBuilder: Option[EmberServerBuilder[F]],
    metricsEventHandler: MetricsEventHandler[F],
    serviceEventHandler: ServiceEventHandler[F],
    reportedEventHandler: ReportedEventHandler[F]): Stream[F, Nothing] =
    val dataRouter = HttpDataRouter[F](metricsEventHandler, serviceEventHandler, reportedEventHandler).router

    emberServerBuilder match {
      case None      => Stream.empty.covary[F]
      case Some(esb) =>
        metricsEventHandler.serviceParams.policies.dashboard match {
          // No dashboard policy, or too few points to chart (<= 1): serve the data routes only, no WS feed.
          case None                                => dataOnly(esb, dataRouter)
          case Some(rm) if rm.maxPoints.value <= 1 => dataOnly(esb, dataRouter)
          case Some(rm)                            =>
            val bc = BackendConfig(
              serviceName = metricsEventHandler.serviceParams.serviceIdentity.service.value,
              zoneId = metricsEventHandler.serviceParams.serviceIdentity.launchTime.zoneId,
              maxPoints = rm.maxPoints,
              policy = rm.policy
            )
            Stream.eval(wsRouter(bc, metricsEventHandler)).flatMap { case (ws, updates) =>
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
