package com.github.chenharryhua.nanjin.guard.service

import cats.data.Kleisli
import cats.effect.kernel.{Async, Ref}
import cats.effect.std.AtomicCell
import cats.syntax.apply.catsSyntaxApplyOps
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsSnapshot, ReportedEvent, ServicePanic}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.Adhoc
import com.github.chenharryhua.nanjin.guard.event.{Event, ScrapeMode, Snapshot, StopReason}
import com.github.chenharryhua.nanjin.guard.logging.LogSink
import com.github.chenharryhua.nanjin.guard.translator.{interpretServiceParams, SnapshotPolyglot}
import fs2.concurrent.Channel
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
  metricRegistry: MetricRegistry,
  panicHistory: AtomicCell[F, CircularFifoQueue[ServicePanic]],
  metricsHistory: AtomicCell[F, CircularFifoQueue[MetricsSnapshot]],
  errorHistory: AtomicCell[F, CircularFifoQueue[ReportedEvent]],
  alarmLevel: Ref[F, Option[AlarmLevel]],
  channel: Channel[F, Event],
  logSink: LogSink[F])(implicit F: Async[F])
    extends Http4sDsl[F] with all {

  private val indexHtml: Text.TypedTag[String] = html(
    head(tag("title")(serviceParams.serviceName.value)),
    body(
      h3(s"Service: ${serviceParams.serviceName.value}"),
      a(href := "/metrics/yaml")("Metrics At Present"),
      br(),
      a(href := "/metrics/json")("Metrics At Present(Json)"),
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
      metricRegistry = metricRegistry,
      panicHistory = panicHistory,
      metricsHistory = metricsHistory,
      errorHistory = errorHistory)

  private val metrics = HttpRoutes.of[F] {
    case GET -> Root                => Ok(indexHtml)
    case GET -> Root / "index.html" => Ok(indexHtml)

    case GET -> Root / "metrics" / "yaml"    => Ok(helper.metrics_yaml)
    case GET -> Root / "metrics" / "vanilla" => Ok(helper.metrics_vanilla)
    case GET -> Root / "metrics" / "json"    => Ok(helper.metrics_json)
    case GET -> Root / "metrics" / "raw"     => Ok(helper.metrics_raw_json)

    case GET -> Root / "metrics" / "reset" =>
      for {
        ts <- serviceParams.zonedNow
        _ <- publish_metrics_reset[F](serviceParams, channel, logSink, metricRegistry, Adhoc(ts))
        (fd, yaml) <- Snapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ms) =>
          (fd, new SnapshotPolyglot(ms).toYaml)
        }
        response <- Ok(html(helper.html_header, body(div(helper.html_table_title(ts, fd), pre(yaml)))))
      } yield response

    case GET -> Root / "metrics" / "jvm"     => Ok(helper.jvm_state)
    case GET -> Root / "metrics" / "history" => Ok(helper.metrics_history)

    // service part

    case GET -> Root / "service" / "params"            => Ok(interpretServiceParams(serviceParams))
    case GET -> Root / "service" / "panic" / "history" => Ok(helper.service_panic_history)
    case GET -> Root / "service" / "error" / "history" => Ok(helper.service_error_history)

    case GET -> Root / "service" / "stop" =>
      val stopping = html(
        head(
          meta(attr("http-equiv") := "refresh", attr("content") := "3;url=/"),
          tag("title")(serviceParams.serviceName.value)),
        body(h1("Stopping Service"))
      )

      Ok(stopping) <* publish_service_stop[F](serviceParams, channel, logSink, StopReason.Maintenance)

    case GET -> Root / "service" / "health_check" =>
      helper.service_health_check.flatMap {
        case Left(value)  => ServiceUnavailable(value)
        case Right(value) => Ok(value)
      }

    case GET -> Root / "alarm_level" =>
      Ok(alarmLevel.get.map {
        case Some(value) => value.entryName
        case None        => "disabled"
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
