package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.effect.kernel.Async
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.json
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.{LogThreshold, Timestamp}
import com.github.chenharryhua.nanjin.guard.event.StopReason
import com.github.chenharryhua.nanjin.guard.service.{
  MetricsEventHandler,
  ReportedEventHandler,
  ServiceEventHandler
}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*
import org.http4s.{HttpRoutes, Response, Uri}

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

  private def update_log_threshold(
    level: String,
    update: (LogThreshold, LogLevel) => LogThreshold
  ): F[Response[F]] =
    LogLevel.values.find(_.toString.equalsIgnoreCase(level)) match {
      case Some(log_level) =>
        reportedEventHandler.logThreshold.getAndUpdate(update(_, log_level))
          .flatMap(previous =>
            Ok(
              Json.obj(
                "previous" -> previous.asJson,
                "current" -> update(previous, log_level).asJson
              )))
      case None =>
        BadRequest(
          Json.obj(
            "invalid_log_level" -> level.asJson,
            "valid" -> LogLevel.values.map(_.toString).toList.asJson
          ))
    }

  val router: HttpRoutes[F] = HttpRoutes.of[F] {

    /*
     * Top Level
     */
    case GET -> Root / "panics" =>
      val json = for {
        now <- F.realTimeInstant.map(Timestamp(serviceParams.serviceIdentity.timeZone, _))
        panics <- serviceEventHandler.panicHistory
      } yield documents.service_panic_history(serviceParams, panics, now)
      Ok(json)

    case GET -> Root / "errors" =>
      val json = for {
        now <- F.realTimeInstant.map(Timestamp(serviceParams.serviceIdentity.timeZone, _))
        panics <- reportedEventHandler.errorHistory
      } yield documents.service_error_history(serviceParams, panics, now)
      Ok(json)

    case GET -> Root / "params" =>
      Ok(
        reportedEventHandler.logThreshold.get
          .map(logThreshold => interpretServiceParams(serviceParams, logThreshold.asJson)))

    case POST -> Root / "stop" =>
      Ok(serviceEventHandler.serviceStop(StopReason.Maintenance).as("Stopping"))

    case GET -> HealthPath() =>
      val or: F[Either[String, Json]] = for {
        panics <- serviceEventHandler.panicHistory
        snapshots <- metricsEventHandler.snapshotHistory
        now <- F.realTimeInstant.map(Timestamp(serviceParams.serviceIdentity.timeZone, _))
      } yield documents.service_health_check(panics, snapshots, now.value.toInstant)

      or.flatMap {
        case Left(value)  => ServiceUnavailable(value)
        case Right(value) => Ok(value)
      }

    /*
     * Metrics
     */

    case GET -> Root / "metrics" / "jvm" =>
      val prettified = json.prettify(mxBeans.allJvmGauge.value.asJson)
      Ok(prettified)

    case GET -> Root / "metrics" / "report" =>
      val text = metricsEventHandler.httpReport.map(documents.snapshot_to_yaml_html("Report", serviceParams))
      Ok(text)

    case GET -> Root / "metrics" / "history" =>
      val text = for {
        now <- F.realTimeInstant.map(Timestamp(serviceParams.serviceIdentity.timeZone, _))
        metrics <- metricsEventHandler.snapshotHistory
      } yield documents.metrics_history(serviceParams, metrics, now.value)
      Ok(text)

    /*
     * Realtime Log Level
     */

    case GET -> Root / "log" / "level" =>
      Ok(reportedEventHandler.logThreshold.get.map(_.asJson))

    case POST -> Root / "log" / "logger" / level =>
      update_log_threshold(level, (threshold, log_level) => threshold.copy(logger = log_level))

    case POST -> Root / "log" / "channel" / level =>
      update_log_threshold(level, (threshold, log_level) => threshold.copy(channel = log_level))
  }
}
