package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.effect.kernel.Async
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.event.StopReason
import com.github.chenharryhua.nanjin.guard.service.{
  MetricsEventHandler,
  ReportedEventHandler,
  ServiceEventHandler
}
import com.github.chenharryhua.nanjin.guard.translator.{interpretServiceParams, prettifyJson}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*
import org.http4s.{HttpRoutes, Uri}

private object HealthPath {
  private val paths: Set[Uri.Path] = Set(
    Uri.Path.unsafeFromString("/health_check"),
    Uri.Path.unsafeFromString("/healthCheck"),
    Uri.Path.unsafeFromString("/healthcheck"),
    Uri.Path.unsafeFromString("/health"),
    Uri.Path.unsafeFromString("/healthz"),
    Uri.Path.unsafeFromString("/healthcheck/status")
  )

  def unapply(path: Uri.Path): Boolean = paths(path)
}

final private class HttpDataRouter[F[_]](
  metricsEventHandler: MetricsEventHandler[F],
  serviceEventHandler: ServiceEventHandler[F],
  reportedEventHandler: ReportedEventHandler[F]
)(using F: Async[F])
    extends Http4sDsl[F] {
  private val serviceParams = metricsEventHandler.serviceParams

  private val DISABLED = "Disabled"
  private def toJson(level: Option[LogLevel]): Json =
    level.fold(DISABLED.asJson)(_.asJson)

  val router: HttpRoutes[F] = HttpRoutes.of[F] {

    /*
     * Top Level
     */
    case GET -> Root / "panics" =>
      val json = for {
        now <- serviceParams.zonedNow
        panics <- serviceEventHandler.panicHistory
      } yield documents.service_panic_history(serviceParams, panics, now)
      Ok(json)

    case GET -> Root / "errors" =>
      val json = for {
        now <- serviceParams.zonedNow
        panics <- reportedEventHandler.errorHistory
      } yield documents.service_error_history(serviceParams, panics, now)
      Ok(json)

    case GET -> Root / "params" =>
      Ok(interpretServiceParams(serviceParams))

    case POST -> Root / "stop" =>
      Ok(serviceEventHandler.serviceStop(StopReason.Maintenance).as("Stopping"))

    case GET -> HealthPath() =>
      val or: F[Either[String, Json]] = for {
        panics <- serviceEventHandler.panicHistory
        snapshots <- metricsEventHandler.snapshotHistory
        now <- serviceParams.zonedNow
      } yield documents.service_health_check(panics, snapshots, now.toInstant)

      or.flatMap {
        case Left(value)  => ServiceUnavailable(value)
        case Right(value) => Ok(value)
      }

    /*
     * Metrics
     */

    case GET -> Root / "metrics" / "jvm" =>
      val json = prettifyJson(mxBeans.allJvmGauge.value.asJson)
      Ok(json)

    case GET -> Root / "metrics" / "report" =>
      val text = metricsEventHandler.httpReport.map(documents.snapshot_to_yaml_html("Report"))
      Ok(text)

    case GET -> Root / "metrics" / "history" =>
      val text = for {
        now <- serviceParams.zonedNow
        metrics <- metricsEventHandler.snapshotHistory
      } yield documents.metrics_history(serviceParams, metrics, now)
      Ok(text)

    /*
     * Realtime Log Level
     */

    case GET -> Root / "log" / "level" =>
      Ok(reportedEventHandler.logThreshold.get.map(toJson))

    case POST -> Root / "log" / level =>
      if level.equalsIgnoreCase(DISABLED) then
        reportedEventHandler.logThreshold.getAndSet(None)
          .flatMap(prev =>
            Ok(
              Json.obj(
                "previous" -> toJson(prev),
                "current" -> toJson(None)
              )))
      else
        LogLevel.values.find(_.toString.equalsIgnoreCase(level)) match {
          case lvl @ Some(_) =>
            reportedEventHandler.logThreshold.getAndSet(lvl)
              .flatMap(prev =>
                Ok(
                  Json.obj(
                    "previous" -> toJson(prev),
                    "current" -> toJson(lvl)
                  )))
          case None =>
            BadRequest(
              Json.obj(
                "invalid_log_level" -> level.asJson,
                "valid" -> (DISABLED :: LogLevel.values.map(_.toString).toList).asJson
              ))
        }
  }
}
