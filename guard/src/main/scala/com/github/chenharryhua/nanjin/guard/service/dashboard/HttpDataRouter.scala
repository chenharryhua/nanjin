package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.effect.kernel.Async
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.guard.config.AlarmLevel
import com.github.chenharryhua.nanjin.guard.event.StopReason
import com.github.chenharryhua.nanjin.guard.service.{
  MetricsEventHandler,
  ReportedEventHandler,
  ServiceEventHandler
}
import com.github.chenharryhua.nanjin.guard.translator.{interpretServiceParams, prettifyJson}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*

final private class HttpDataRouter[F[_]](
  metricsEventHandler: MetricsEventHandler[F],
  serviceEventHandler: ServiceEventHandler[F],
  reportedEventHandler: ReportedEventHandler[F]
)(using F: Async[F])
    extends Http4sDsl[F] {
  private val serviceParams = metricsEventHandler.serviceParams

  private val DISABLED = "Disabled"
  private def toJson(level: Option[AlarmLevel]): Json =
    level.fold(DISABLED.asJson)(_.asJson)

  val router: HttpRoutes[F] = HttpRoutes.of[F] {

    /*
     * Panics and Errors
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

    /*
     * Service
     */

    case GET -> Root / "service" / "params" =>
      Ok(interpretServiceParams(serviceParams))

    case GET -> Root / "service" / "stop" =>
      Ok(serviceEventHandler.serviceStop(StopReason.Maintenance).as("Stopping"))

    case GET -> Root / "service" / "jvm" =>
      val json = prettifyJson(mxBeans.allJvmGauge.value.asJson)
      Ok(json)

    case GET -> Root / "service" / "health_check" =>
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

    case GET -> Root / "metrics" / "report" =>
      val text = metricsEventHandler.httpReport.map(documents.snapshot_to_yaml_html("Report"))
      Ok(text)

    case GET -> Root / "metrics" / "reset" =>
      val text = metricsEventHandler.httpReset.map(documents.snapshot_to_yaml_html("Reset"))
      Ok(text)

    case GET -> Root / "metrics" / "history" =>
      val text = for {
        now <- serviceParams.zonedNow
        metrics <- metricsEventHandler.snapshotHistory
      } yield documents.metrics_history(serviceParams, metrics, now)
      Ok(text)

    /*
     * Realtime Alarm Level
     */

    case GET -> Root / "alarm" / "level" =>
      Ok(reportedEventHandler.alarmThreshold.get.map(toJson))

    case POST -> Root / "alarm" / level =>
      if level.equalsIgnoreCase(DISABLED) then
        reportedEventHandler.alarmThreshold.getAndSet(None)
          .flatMap(prev =>
            Ok(
              Json.obj(
                "previous" -> toJson(prev),
                "current" -> toJson(None)
              )))
      else
        AlarmLevel.values.find(_.toString.equalsIgnoreCase(level)) match {
          case lvl @ Some(_) =>
            reportedEventHandler.alarmThreshold.getAndSet(lvl)
              .flatMap(prev =>
                Ok(
                  Json.obj(
                    "previous" -> toJson(prev),
                    "current" -> toJson(lvl)
                  )))
          case None =>
            BadRequest(
              Json.obj(
                "invalid_alarm_level" -> level.asJson,
                "valid" -> (DISABLED :: AlarmLevel.values.map(_.toString).toList).asJson
              ))
        }
  }
}
