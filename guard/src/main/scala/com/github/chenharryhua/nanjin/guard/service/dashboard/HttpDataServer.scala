package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.data.Kleisli
import cats.effect.kernel.Async
import cats.effect.std.AtomicCell
import cats.effect.{Ref, Resource}
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.comcast.ip4s.Port
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import com.github.chenharryhua.nanjin.common.resilience.SingleFlight
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.{MetricID, StopReason}
import com.github.chenharryhua.nanjin.guard.service.{LifecyclePublisher, MetricsPublisher}
import com.github.chenharryhua.nanjin.guard.translator.{interpretServiceParams, prettifyJson}
import fs2.Stream
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.scalatags.*
import org.http4s.server.Router
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.{HttpRoutes, Request, Response, StaticFile}
import scalatags.Text
import scalatags.Text.all.*

import scala.jdk.CollectionConverters.IteratorHasAsScala

final private class HttpDataServer[F[_]](
  pump: MetricsPump[F],
  port: Port,
  metricsPublisher: MetricsPublisher[F],
  lifecyclePublisher: LifecyclePublisher[F],
  errorHistory: AtomicCell[F, CircularFifoQueue[ReportedEvent]])(using F: Async[F])
    extends Http4sDsl[F] {

  private val serviceParams = metricsPublisher.serviceParams

  private val inject_backend_script = BackendConfig(
    serviceName = serviceParams.serviceName.value,
    port = port,
    zoneId = serviceParams.zoneId,
    maxPoints = serviceParams.servicePolicies.realtimeMetrics.maxPoints,
    policy = serviceParams.servicePolicies.realtimeMetrics.policy
  )

  private val html_page: Text.TypedTag[String] =
    html(
      head(
        tag("title")(serviceParams.serviceName.value),
        script(src := "https://cdn.jsdelivr.net/npm/chart.js"),
        script(src := "https://cdn.jsdelivr.net/npm/luxon"),
        script(src := "https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon"),
        link(rel   := "icon", href := "/dashboard/favicon.ico", `type` := "image/x-icon"),
        inject_backend_script.config
      ),
      body(
        div(id        := "dashboard"), // mount point
        script(`type` := "module", src := "/dashboard/nj-frontend.js"))
    )

  private def control_center(wsb2: WebSocketBuilder2[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      /*
       * Dashboard and Websocket
       */
      case GET -> Root / "dashboard" / file =>
        StaticFile.fromResource(s"dashboard/$file").getOrElseF(NotFound())

      case GET -> Root / "dashboard" => Ok(html_page)
      case GET -> Root / "ws"        => pump.pumping(wsb2)

      /*
       * Panics and Errors
       */
      case GET -> Root / "panics" =>
        val json = for {
          now <- serviceParams.zonedNow
          panics <- lifecyclePublisher.get_panic_history
        } yield documents.service_panic_history(serviceParams, panics, now)
        Ok(json)

      case GET -> Root / "errors" =>
        val json = for {
          now <- serviceParams.zonedNow
          panics <- errorHistory.get.map(_.iterator().asScala.toList)
        } yield documents.service_error_history(serviceParams, panics, now)
        Ok(json)

      /*
       * Service
       */

      case GET -> Root / "service" / "params" =>
        Ok(interpretServiceParams(serviceParams))

      case GET -> Root / "service" / "stop" =>
        Ok(lifecyclePublisher.service_stop(StopReason.Maintenance).as("Stopping"))

      case GET -> Root / "service" / "jvm" =>
        val json = prettifyJson(mxBeans.allJvmGauge.value.asJson)
        Ok(json)

      case GET -> Root / "service" / "health_check" =>
        val or: F[Either[String, Json]] = for {
          panics <- lifecyclePublisher.get_panic_history
          snapshots <- metricsPublisher.get_snapshot_history
          now <- serviceParams.zonedNow
        } yield documents.service_health_check(panics, snapshots, now.toInstant)

        or.flatMap {
          case Left(value)  => ServiceUnavailable(value)
          case Right(value) => Ok(value)
        }

      /*
       * Metrics
       */

      case GET -> Root / "metrics" / "report" =>
        val text = metricsPublisher.report_adhoc.map(documents.snapshot_to_yaml_html("Report"))
        Ok(text)

      case GET -> Root / "metrics" / "reset" =>
        val text = metricsPublisher.reset_adhoc.map(documents.snapshot_to_yaml_html("Reset"))
        Ok(text)

      case GET -> Root / "metrics" / "history" =>
        val text = for {
          now <- serviceParams.zonedNow
          metrics <- metricsPublisher.get_snapshot_history
        } yield documents.metrics_history(serviceParams, metrics, now)
        Ok(text)
    }

  def dataRouter(wsb2: WebSocketBuilder2[F]): Kleisli[F, Request[F], Response[F]] =
    Router("/" -> control_center(wsb2)).orNotFound
}

private[service] object HttpDataServer {
  def stream[F[_]: Async](
    emberServerBuilder: Option[EmberServerBuilder[F]],
    metricsPublisher: MetricsPublisher[F],
    lifecyclePublisher: LifecyclePublisher[F],
    errorHistory: AtomicCell[F, CircularFifoQueue[ReportedEvent]]): Stream[F, Nothing] =
    emberServerBuilder.fold(Stream.empty.covaryAll[F, Nothing]) { esb =>
      val httpDataServer: F[HttpDataServer[F]] = for {
        zeroth <- Tick.zeroth(metricsPublisher.serviceParams.zoneId)
        ref <- Ref.of[F, TickedValue[Map[MetricID, Long]]](TickedValue(zeroth, Map.empty))
        singleFlight <- SingleFlight[F, TickedValue[Map[MetricID, Long]]]
      } yield {
        val pump = new MetricsPump[F](
          scrapeMetrics = metricsPublisher.scrapeMetrics,
          zoneId = metricsPublisher.serviceParams.zoneId,
          policy = metricsPublisher.serviceParams.servicePolicies.realtimeMetrics.policy,
          ref = ref,
          singleFlight = singleFlight
        )

        new HttpDataServer[F](
          pump = pump,
          port = esb.port,
          metricsPublisher = metricsPublisher,
          lifecyclePublisher = lifecyclePublisher,
          errorHistory = errorHistory)
      }

      Stream.resource {
        Resource.eval(httpDataServer)
          .flatMap(hds => esb.withHttpWebSocketApp(hds.dataRouter).build)
      } >> Stream.never
    }
}
