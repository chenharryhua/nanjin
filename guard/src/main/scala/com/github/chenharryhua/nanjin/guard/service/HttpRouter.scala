package com.github.chenharryhua.nanjin.guard.service

import cats.data.Kleisli
import cats.effect.kernel.{Async, Clock}
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricReport, ServicePanic}
import com.github.chenharryhua.nanjin.guard.event.{
  retrieveHealthChecks,
  Event,
  MetricIndex,
  MetricSnapshot,
  ServiceStopCause
}
import com.github.chenharryhua.nanjin.guard.translator.htmlHelper.htmlColoring
import com.github.chenharryhua.nanjin.guard.translator.textConstants.CONSTANT_LAUNCH_TIME
import com.github.chenharryhua.nanjin.guard.translator.textConstants.CONSTANT_TOOK
import com.github.chenharryhua.nanjin.guard.translator.{fmt, prettifyJson, SnapshotPolyglot}
import fs2.concurrent.Channel
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*
import org.http4s.server.Router
import org.http4s.{HttpRoutes, Request, Response}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import scala.jdk.CollectionConverters.IteratorHasAsScala

private class HttpRouter[F[_]](
  metricRegistry: MetricRegistry,
  serviceParams: ServiceParams,
  panicHistory: AtomicCell[F, CircularFifoQueue[ServicePanic]],
  metricsHistory: AtomicCell[F, CircularFifoQueue[MetricReport]],
  channel: Channel[F, Event])(implicit F: Async[F])
    extends Http4sDsl[F] with all {

  private val html_header: Text.TypedTag[String] =
    head(tag("style")("""
        td, th {text-align: left; padding: 2px; border: 1px solid;}
        table {
          border-collapse: collapse;
          width: 60%;
        }
      """))

  private def html_table_title(now: ZonedDateTime): Text.TypedTag[String] =
    table(
      tr(th("Service"), th("Report Policy"), th("Time Zone"), th("Up Time"), th("Present")),
      tr(
        td(serviceParams.serviceName.value),
        td(serviceParams.servicePolicies.metricReport.show),
        td(serviceParams.zoneId.show),
        td(fmt.format(serviceParams.upTime(now))),
        td(now.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)
      )
    )

  private val deps_health_check: F[Json] =
    serviceParams.zonedNow[F].flatMap { now =>
      F.timed(F.delay(retrieveHealthChecks(metricRegistry).values.forall(identity))).map { case (fd, b) =>
        Json.obj(
          "healthy" -> b.asJson,
          "took" -> fmt.format(fd).asJson,
          "when" -> now.toLocalTime.truncatedTo(ChronoUnit.SECONDS).asJson)
      }
    }

  private val metrics = HttpRoutes.of[F] {
    case GET -> Root / "index.html" =>
      val text: Text.TypedTag[String] = html(
        body(
          h3(s"Service: ${serviceParams.serviceName.value}"),
          a(href := "/metrics/yaml")("Metrics At Present"),
          br(),
          a(href := "/metrics/json")("Metrics At Present(Json)"),
          br(),
          a(href := "/metrics/history")("Metrics History"),
          br(),
          a(href := "/metrics/reset")("Metrics Counters Reset"),
          br(),
          a(href := "/metrics/jvm")("Jvm"),
          br(),
          a(href := "/service/params")("Service Parameters"),
          br(),
          a(href := "/service/history")("Service Panic History"),
          br(),
          a(href := "/service/health_check")("Service Health Check"),
          br(),
          br(),
          form(action := "/service/stop")(
            input(`type` := "submit", onclick := "return confirm('Are you sure?')", value := "Stop Service"))
        ))
      Ok(text)

    case GET -> Root / "metrics" / "yaml" =>
      val text: F[Text.TypedTag[String]] =
        serviceParams.zonedNow.map { now =>
          val yaml = new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toYaml
          html(html_header, body(div(html_table_title(now), pre(yaml))))
        }
      Ok(text)

    case GET -> Root / "metrics" / "vanilla" =>
      val vanilla = new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toVanillaJson
      Ok(vanilla)

    case GET -> Root / "metrics" / "json" =>
      val json = new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toPrettyJson
      Ok(json)

    case GET -> Root / "metrics" / "raw" =>
      val json = MetricSnapshot(metricRegistry).asJson
      Ok(json)

    case GET -> Root / "metrics" / "reset" =>
      for {
        ts <- serviceParams.zonedNow
        _ <- publisher.metricReset[F](channel, serviceParams, metricRegistry, MetricIndex.Adhoc(ts))
        yaml = new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toYaml
        response <- Ok(html(html_header, body(div(html_table_title(ts), pre(yaml)))))
      } yield response

    case GET -> Root / "metrics" / "jvm" =>
      Ok(prettifyJson(mxBeans.allJvmGauge.value.asJson))

    case GET -> Root / "metrics" / "history" =>
      val text: F[Text.TypedTag[String]] =
        serviceParams.zonedNow.flatMap { now =>
          val history: F[List[Text.TypedTag[String]]] =
            metricsHistory.get.map(_.iterator().asScala.toList.reverse.flatMap { mr =>
              mr.index match {
                case _: MetricIndex.Adhoc => None
                case MetricIndex.Periodic(tick) =>
                  Some(
                    div(
                      h3(style := htmlColoring(mr))("Report Index: ", tick.index),
                      table(
                        tr(th(CONSTANT_LAUNCH_TIME), th(CONSTANT_TOOK)),
                        tr(td(tick.zonedWakeup.toLocalDateTime.show), td(fmt.format(mr.took)))
                      ),
                      pre(new SnapshotPolyglot(mr.snapshot).toYaml)
                    ))
              }
            })
          history.map(hist => div(html_table_title(now), hist))
        }

      Ok(text.map(t => html(html_header, body(t))))

    // service part

    case GET -> Root / "service" / "params" => Ok(serviceParams.asJson)

    case GET -> Root / "service" / "stop" =>
      Ok("stopping service") <* publisher.serviceStop[F](channel, serviceParams, ServiceStopCause.Maintenance)

    case GET -> Root / "service" / "health_check" =>
      panicHistory.get.map(_.iterator().asScala.toList.lastOption).flatMap {
        case None => deps_health_check.flatMap(Ok(_))
        case Some(evt) =>
          Clock[F].realTimeInstant.flatMap { now =>
            if (evt.tick.wakeup.isAfter(now)) {
              val recover = Duration.between(now, evt.tick.wakeup)
              ServiceUnavailable(s"Service panic! Restart will be in ${fmt.format(recover)}")
            } else {
              deps_health_check.flatMap(Ok(_))
            }
          }
      }

    case GET -> Root / "service" / "history" =>
      serviceParams.zonedNow.flatMap { now =>
        panicHistory.get.map(_.iterator().asScala.toList).flatMap { panics =>
          val json: Json =
            Json.obj(
              "restart_policy" -> serviceParams.servicePolicies.restart.show.asJson,
              "zone_id" -> serviceParams.zoneId.asJson,
              "up_time" -> fmt.format(serviceParams.upTime(now)).asJson,
              "history" ->
                panics.reverse.map { sp =>
                  Json.obj(
                    "index" -> sp.tick.index.asJson,
                    "uprouse_at" -> sp.tick.zonedPrevious.toLocalDateTime.asJson,
                    "active" -> fmt.format(sp.tick.active).asJson,
                    "when_panic" -> sp.tick.zonedAcquire.toLocalDateTime.asJson,
                    "snooze" -> fmt.format(sp.tick.snooze).asJson,
                    "restart_at" -> sp.tick.zonedWakeup.toLocalDateTime.asJson,
                    "caused_by" -> sp.error.message.asJson
                  )
                }.asJson
            )
          Ok(json)
        }
      }
  }

  val router: Kleisli[F, Request[F], Response[F]] =
    Router("/" -> metrics).orNotFound
}
