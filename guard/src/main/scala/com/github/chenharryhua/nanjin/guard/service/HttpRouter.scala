package com.github.chenharryhua.nanjin.guard.service

import cats.data.Kleisli
import cats.effect.kernel.{Async, Clock}
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{MetricReport, ServicePanic}
import com.github.chenharryhua.nanjin.guard.event.{
  retrieveHealthChecks,
  MetricIndex,
  MetricSnapshot,
  NJEvent,
  ServiceStopCause
}
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

import java.time.Duration
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters.IteratorHasAsScala

private class HttpRouter[F[_]](
  metricRegistry: MetricRegistry,
  serviceParams: ServiceParams,
  panicHistory: AtomicCell[F, CircularFifoQueue[ServicePanic]],
  metricsHistory: AtomicCell[F, CircularFifoQueue[MetricReport]],
  channel: Channel[F, NJEvent])(implicit F: Async[F])
    extends Http4sDsl[F] with all {

  private val dependenciesHealthCheck: F[Json] =
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
          a(href := "/metrics/yaml")(p("Metrics")),
          a(href := "/metrics/history")(p("Metrics History")),
          a(href := "/metrics/reset")(p("Metrics Counters Reset")),
          a(href := "/metrics/jvm")(p("Jvm Gauge")),
          a(href := "/service/params")(p("Service Parameters")),
          a(href := "/service/history")(p("Service Panic History")),
          a(href := "/service/health_check")(p("Service Health Check")),
          br(),
          br(),
          form(action := "/service/stop")(
            input(`type` := "submit", onclick := "return confirm('Are you sure?')", value := "Stop Service"))
        ))
      Ok(text)

    case GET -> Root / "metrics" / "yaml" =>
      val text = new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toYaml
      Ok(html(body(pre(text))))

    case GET -> Root / "metrics" / "vanilla" =>
      Ok(new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toVanillaJson)

    case GET -> Root / "metrics" / "json" =>
      Ok(new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toPrettyJson)

    case GET -> Root / "metrics" / "reset" =>
      for {
        ts <- serviceParams.zonedNow
        _ <- publisher.metricReset[F](channel, serviceParams, metricRegistry, MetricIndex.Adhoc(ts))
        response <- Ok(html(body(pre(new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toYaml))))
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
                      h3("Report Index: ", tick.index),
                      table(
                        tr(td(b("Launch Time")), td(b("Took"))),
                        tr(td(tick.zonedWakeup.toLocalDateTime.show), td(fmt.format(mr.took)))),
                      pre(small(new SnapshotPolyglot(mr.snapshot).toYaml))
                    ))
              }
            })
          history.map { hist =>
            div(
              table(
                tr(td(b("Service")), td(b("Report Policy")), td(b("Time Zone")), td(b("Up Time"))),
                tr(
                  td(serviceParams.serviceName.value),
                  td(serviceParams.servicePolicies.metricReport.show),
                  td(serviceParams.zoneId.show),
                  td(fmt.format(serviceParams.upTime(now)))
                )
              ),
              hist
            )
          }
        }
      val header: Text.TypedTag[String] = head(tag("style")("""
        td, th {text-align: left; padding: 2px; border: 1px solid;}
        table {
          border-collapse: collapse;
          width: 50%;
        }
      """))
      Ok(text.map(t => html(header, body(t))))

    // service part

    case GET -> Root / "service" / "params" => Ok(serviceParams.asJson)

    case GET -> Root / "service" / "stop" =>
      Ok("stopping service") <* publisher.serviceStop[F](channel, serviceParams, ServiceStopCause.Maintenance)

    case GET -> Root / "service" / "health_check" =>
      panicHistory.get.map(_.iterator().asScala.toList.lastOption).flatMap {
        case None => dependenciesHealthCheck.flatMap(Ok(_))
        case Some(evt) =>
          Clock[F].realTimeInstant.flatMap { now =>
            if (evt.tick.wakeup.isAfter(now)) {
              val recover = Duration.between(now, evt.tick.wakeup)
              ServiceUnavailable(s"Service panic! Restart will be in ${fmt.format(recover)}")
            } else {
              dependenciesHealthCheck.flatMap(Ok(_))
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
                    "took_place" -> sp.tick.zonedAcquire.toLocalDateTime.asJson,
                    "restart_at" -> sp.tick.zonedWakeup.toLocalDateTime.asJson,
                    "snooze" -> fmt.format(sp.tick.snooze).asJson,
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
