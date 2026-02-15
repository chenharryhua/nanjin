package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Clock, Sync}
import cats.effect.std.AtomicCell
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.either.catsSyntaxEitherId
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.show.toShow
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricReport, ServiceMessage, ServicePanic}
import com.github.chenharryhua.nanjin.guard.event.{
  retrieveHealthChecks,
  MetricIndex,
  MetricSnapshot,
  ScrapeMode
}
import com.github.chenharryhua.nanjin.guard.translator.htmlHelper.htmlColoring
import com.github.chenharryhua.nanjin.guard.translator.textConstants.{CONSTANT_TIMESTAMP, CONSTANT_TOOK}
import com.github.chenharryhua.nanjin.guard.translator.{durationFormatter, prettifyJson, SnapshotPolyglot}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import scala.jdk.CollectionConverters.IteratorHasAsScala

final private class HttpRouterHelper[F[_]: Sync](
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  panicHistory: AtomicCell[F, CircularFifoQueue[ServicePanic]],
  metricsHistory: AtomicCell[F, CircularFifoQueue[MetricReport]],
  errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]])
    extends all {

  val html_header: Text.TypedTag[String] =
    head(
      tag("style")("""
        td, th {text-align: left; padding: 2px; border: 1px solid;}
        table {
          border-collapse: collapse;
          width: 70%;
        }
      """),
      tag("title")(serviceParams.serviceName.value)
    )

  def html_table_title(now: ZonedDateTime, took: Duration): Text.TypedTag[String] =
    table(
      tr(th("Service"), th("Report Policy"), th("Time Zone"), th("Up Time"), th("Present"), th("Took")),
      tr(
        td(serviceParams.serviceName.value),
        td(serviceParams.servicePolicies.metricReport.show),
        td(serviceParams.zoneId.show),
        td(durationFormatter.format(serviceParams.upTime(now))),
        td(now.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show),
        td(durationFormatter.format(took))
      )
    )

  val metrics_yaml: F[Text.TypedTag[String]] =
    serviceParams.zonedNow.flatMap { now =>
      MetricSnapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ms) =>
        val yaml = new SnapshotPolyglot(ms).toYaml
        html(html_header, body(div(html_table_title(now, fd), pre(yaml))))
      }
    }

  val metrics_vanilla: F[Json] =
    MetricSnapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ms) =>
      Json.obj(
        "service" -> Json.fromString(serviceParams.serviceName.value),
        "took" -> Json.fromString(durationFormatter.format(fd)),
        "snapshot" -> new SnapshotPolyglot(ms).toVanillaJson
      )
    }

  val metrics_json: F[Json] =
    MetricSnapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ms) =>
      Json.obj(
        "service" -> Json.fromString(serviceParams.serviceName.value),
        "took" -> Json.fromString(durationFormatter.format(fd)),
        "snapshot" -> new SnapshotPolyglot(ms).toPrettyJson
      )
    }

  val metrics_raw_json: F[Json] =
    MetricSnapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ms) =>
      Json.obj(
        "service" -> Json.fromString(serviceParams.serviceName.value),
        "took" -> Json.fromString(durationFormatter.format(fd)),
        "snapshot" -> ms.sorted.asJson)
    }

  val metrics_history: F[Text.TypedTag[String]] = {
    val text: F[Text.TypedTag[String]] =
      serviceParams.zonedNow.flatMap { now =>
        val history: F[List[Text.TypedTag[String]]] =
          metricsHistory.get.map(_.iterator().asScala.toList.reverse.flatMap { mr =>
            mr.index match {
              case _: MetricIndex.Adhoc       => None
              case MetricIndex.Periodic(tick) =>
                Some(
                  div(
                    h3(style := htmlColoring(mr))("Report Index: ", tick.index),
                    table(
                      tr(th(CONSTANT_TIMESTAMP), th(CONSTANT_TOOK)),
                      tr(td(tick.local(_.conclude).show), td(durationFormatter.format(mr.took)))
                    ),
                    pre(new SnapshotPolyglot(mr.snapshot).toYaml)
                  ))
            }
          })
        history.map(hist => div(html_table_title(now, Duration.ZERO), hist))
      }

    text.map(t => html(html_header, body(div(t))))
  }

  def jvm_state: Json = prettifyJson(mxBeans.allJvmGauge.value.asJson)

  val service_panic_history: F[Json] =
    serviceParams.zonedNow.flatMap { now =>
      panicHistory.get.map(_.iterator().asScala.toList).map { panics =>
        val isActive = panics.lastOption.map(_.tick.conclude).forall(_.isBefore(now.toInstant))

        Json.obj(
          "service" -> Json.fromString(serviceParams.serviceName.value),
          "service_id" -> Json.fromString(serviceParams.serviceId.show),
          "is_active" -> Json.fromBoolean(isActive),
          "present" -> now.toLocalTime.truncatedTo(ChronoUnit.SECONDS).asJson,
          "restart_policy" -> serviceParams.servicePolicies.restart.policy.show.asJson,
          "zone_id" -> serviceParams.zoneId.asJson,
          "up_time" -> durationFormatter.format(serviceParams.upTime(now)).asJson,
          "panics" -> panics.size.asJson,
          "history" ->
            panics.reverse.map { sp =>
              Json.obj(
                "index" -> sp.tick.index.asJson,
                "age" -> durationFormatter.format(Duration.between(sp.timestamp, now)).asJson,
                "up_rouse_at" -> sp.tick.local(_.commence).asJson,
                "active" -> durationFormatter.format(sp.tick.active).asJson,
                "when_panic" -> sp.tick.local(_.acquires).asJson,
                "snooze" -> durationFormatter.format(sp.tick.snooze).asJson,
                "restart_at" -> sp.tick.local(_.conclude).asJson,
                "caused_by" -> sp.error.message.asJson
              )
            }.asJson
        )
      }
    }

  val service_error_history: F[Json] =
    serviceParams.zonedNow.flatMap { now =>
      errorHistory.get.map(_.iterator().asScala.toList).map { serviceMessages =>
        Json.obj(
          "service" -> Json.fromString(serviceParams.serviceName.value),
          "service_id" -> Json.fromString(serviceParams.serviceId.show),
          "present" -> now.toLocalTime.truncatedTo(ChronoUnit.SECONDS).asJson,
          "zone_id" -> serviceParams.zoneId.asJson,
          "up_time" -> durationFormatter.format(serviceParams.upTime(now)).asJson,
          "errors" -> serviceMessages.size.asJson,
          "history" -> serviceMessages.reverse.map { sm =>
            sm.error
              .map(err => Json.obj("stack" -> err.stack.asJson))
              .asJson
              .deepMerge(Json.obj(
                "domain" -> sm.domain.asJson,
                "token" -> sm.token.asJson,
                "age" -> durationFormatter.format(Duration.between(sm.timestamp, now)).asJson,
                "timestamp" -> sm.timestamp.asJson,
                "message" -> sm.message
              ))
          }.asJson
        )
      }
    }

  val service_health_check: F[Either[String, Json]] = {
    val deps_health_check: F[Json] =
      serviceParams.zonedNow[F].flatMap { now =>
        MetricSnapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ss) =>
          Json.obj(
            "healthy" -> retrieveHealthChecks(ss.gauges).values.forall(identity).asJson,
            "took" -> durationFormatter.format(fd).asJson,
            "when" -> now.toLocalTime.truncatedTo(ChronoUnit.SECONDS).asJson
          )
        }
      }

    panicHistory.get.map(_.iterator().asScala.toList.lastOption).flatMap {
      case None      => deps_health_check.map(Right(_))
      case Some(evt) =>
        Clock[F].realTimeInstant.flatMap { now =>
          if (evt.tick.conclude.isAfter(now)) {
            val recover = Duration.between(now, evt.tick.conclude)
            s"Service panic! Restart will be in ${durationFormatter.format(recover)}".asLeft[Json].pure[F]
          } else {
            deps_health_check.map(Right(_))
          }
        }
    }
  }
}
