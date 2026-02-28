package com.github.chenharryhua.nanjin.guard.service

import cats.data.Kleisli
import cats.effect.kernel.{Async, Ref}
import cats.effect.std.AtomicCell
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.translator.{interpretServiceParams, Attribute}
import io.circe.Json
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*
import org.http4s.server.Router
import org.http4s.{HttpRoutes, Request, Response}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

final private class HttpRouter[F[_]](
  serviceParams: ServiceParams,
  errorHistory: AtomicCell[F, CircularFifoQueue[ReportedEvent]],
  alarmLevel: Ref[F, Option[AlarmLevel]],
  metricsPublisher: MetricsPublisher[F],
  lifecyclePublisher: LifecyclePublisher[F])(implicit F: Async[F])
    extends Http4sDsl[F] with all {

  private val indexHtml: Text.TypedTag[String] = html(
    head(tag("title")(serviceParams.serviceName.value)),
    body(
      h3(s"Service: ${serviceParams.serviceName.value}"),
      a(href := "/metrics/yaml")("Metrics At Present"),
      br(),
      a(href := "/metrics/reset")("Metrics Counters Reset"),
      br(),
      br(),
      a(href := "/metrics/history")("Metrics History"),
      br(),
      a(href := "/service/panic/history")("Panic History"),
      br(),
      a(href := "/service/error/history")("Error History"),
      br(),
      br(),
      a(href := "/service/params")("Service Parameters"),
      br(),
      a(href := "/metrics/jvm")("Java Runtime"),
      br(),
      a(href := "/service/health_check")("Service Health Check"),
      br(),
      a(href := "/alarm_level")("Alarm Level"),
      br(),
      br(),
      form(action := "/service/stop")(
        input(`type` := "submit", onclick := "return confirm('Are you sure?')", value := "Stop Service"))
    )
  )

  private val helper: HttpRouterHelper[F] =
    new HttpRouterHelper[F](
      serviceParams = serviceParams,
      errorHistory = errorHistory,
      metricsPublisher = metricsPublisher,
      lifecyclePublisher = lifecyclePublisher
    )

  private val metrics = HttpRoutes.of[F] {
    case GET -> Root                => Ok(indexHtml)
    case GET -> Root / "index.html" => Ok(indexHtml)

    case GET -> Root / "metrics" / "yaml"  => Ok(helper.metrics_report_yaml)
    case GET -> Root / "metrics" / "reset" => Ok(helper.metrics_reset_yaml)

    case GET -> Root / "metrics" / "jvm"     => Ok(helper.jvm_state)
    case GET -> Root / "metrics" / "history" => Ok(helper.metrics_history)

    // service part

    case GET -> Root / "service" / "params"            => Ok(interpretServiceParams(serviceParams))
    case GET -> Root / "service" / "panic" / "history" => Ok(helper.service_panic_history)
    case GET -> Root / "service" / "error" / "history" => Ok(helper.service_error_history)

    case GET -> Root / "service" / "stop" => Ok(helper.service_stop)

    case GET -> Root / "service" / "health_check" =>
      helper.service_health_check.flatMap {
        case Left(value)  => ServiceUnavailable(value)
        case Right(value) => Ok(value)
      }

    case GET -> Root / "alarm_level" =>
      Ok(alarmLevel.get.map {
        case Some(value) => Json.obj(Attribute(value).snakeJsonEntry)
        case None        => Json.obj("alarm_level" -> Json.fromString("disabled"))
      })
    case GET -> Root / "alarm_level" / AlarmLevel.Debug.entryName => setAlarmLevel(Some(AlarmLevel.Debug))
    case GET -> Root / "alarm_level" / AlarmLevel.Info.entryName  => setAlarmLevel(Some(AlarmLevel.Info))
    case GET -> Root / "alarm_level" / AlarmLevel.Good.entryName  => setAlarmLevel(Some(AlarmLevel.Good))
    case GET -> Root / "alarm_level" / AlarmLevel.Warn.entryName  => setAlarmLevel(Some(AlarmLevel.Warn))
    case GET -> Root / "alarm_level" / AlarmLevel.Error.entryName => setAlarmLevel(Some(AlarmLevel.Error))
    case GET -> Root / "alarm_level" / "disable"                  => setAlarmLevel(None)
  }

  private def setAlarmLevel(level: Option[AlarmLevel]): F[Response[F]] =
    Accepted(
      alarmLevel.getAndSet(level).map { pre =>
        Json.obj(
          "previous" -> Json.fromString(pre.map(_.entryName).getOrElse("disabled")),
          "current" -> Json.fromString(level.map(_.entryName).getOrElse("disabled")))
      }
    )

  val router: Kleisli[F, Request[F], Response[F]] =
    Router("/" -> metrics).orNotFound
}
