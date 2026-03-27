package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.effect.kernel.Async
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.StopReason
import com.github.chenharryhua.nanjin.guard.service.{History, LifecyclePublisher, MetricsPublisher}
import com.github.chenharryhua.nanjin.guard.translator.{interpretServiceParams, prettifyJson}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*

final private class HttpDataRouter[F[_]](
  metricsPublisher: MetricsPublisher[F],
  lifecyclePublisher: LifecyclePublisher[F],
  errorHistory: History[F, ReportedEvent]
)(using F: Async[F])
    extends Http4sDsl[F] {
  private val serviceParams = metricsPublisher.serviceParams

  val router: HttpRoutes[F] = HttpRoutes.of[F] {

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
        panics <- errorHistory.value
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
      val text = metricsPublisher.http_report.map(documents.snapshot_to_yaml_html("Report"))
      Ok(text)

    case GET -> Root / "metrics" / "reset" =>
      val text = metricsPublisher.http_reset.map(documents.snapshot_to_yaml_html("Reset"))
      Ok(text)

    case GET -> Root / "metrics" / "history" =>
      val text = for {
        now <- serviceParams.zonedNow
        metrics <- metricsPublisher.get_snapshot_history
      } yield documents.metrics_history(serviceParams, metrics, now)
      Ok(text)
  }
}
