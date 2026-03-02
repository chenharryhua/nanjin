package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Clock, Sync}
import cats.effect.std.AtomicCell
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.either.catsSyntaxEitherId
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.dashboard.histories
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsSnapshot, ReportedEvent}
import com.github.chenharryhua.nanjin.guard.event.{retrieveHealthChecks, StopReason, Took}
import com.github.chenharryhua.nanjin.guard.translator.{
  durationFormatter,
  prettifyJson,
  Attribute,
  SnapshotPolyglot
}
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
  errorHistory: AtomicCell[F, CircularFifoQueue[ReportedEvent]],
  metricsPublisher: MetricsPublisher[F],
  lifecyclePublisher: LifecyclePublisher[F])
    extends all {

  private case class Present(value: ZonedDateTime) {
    val text: String = value.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show
    val json: Json = text.asJson
  }

  private val html_header: Text.TypedTag[String] =
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

  private def html_table_title(now: ZonedDateTime, took: Duration): Text.TypedTag[String] = {
    val service_name = Attribute(serviceParams.serviceName).textEntry
    val policy = Attribute(serviceParams.servicePolicies.metricsReport).textEntry
    val timezone = Attribute(serviceParams.timeZone).textEntry
    val uptime = Attribute(serviceParams.upTime(now)).textEntry
    val spend = Attribute(Took(took)).textEntry
    val present = Attribute(Present(now)).textEntry(_.text)

    table(
      tr(
        th(service_name.tag),
        th(policy.tag),
        th(timezone.tag),
        th(uptime.tag),
        th(spend.tag),
        th(present.tag)),
      tr(
        td(service_name.text),
        td(policy.text),
        td(timezone.text),
        td(uptime.text),
        td(spend.text),
        td(present.text))
    )
  }

  private def toYaml(ms: MetricsSnapshot): Text.TypedTag[String] = {
    val yaml = new SnapshotPolyglot(ms.snapshot).toYaml
    html(html_header, body(div(html_table_title(ms.timestamp.value, ms.took.value), pre(yaml))))
  }

  val metrics_report_yaml: F[Text.TypedTag[String]] =
    metricsPublisher.report_adhoc.map(toYaml)

  val metrics_reset_yaml: F[Text.TypedTag[String]] =
    metricsPublisher.reset_adhoc.map(toYaml)

  val metrics_history: F[Text.TypedTag[String]] = {
    val text =
      serviceParams.zonedNow.flatMap { now =>
        metricsPublisher.get_snapshot_history.map { mrs =>
          histories.metrics_history(serviceParams, mrs.reverse, now)
        }
      }

    text.map(t => html(html_header, body(div(t))))
  }

  def jvm_state: Json = prettifyJson(mxBeans.allJvmGauge.value.asJson)

  val service_panic_history: F[Json] =
    serviceParams.zonedNow.flatMap { now =>
      lifecyclePublisher.get_panic_history.map { panics =>
        histories.service_panic_history(serviceParams, panics, now)
      }
    }

  val service_error_history: F[Json] =
    serviceParams.zonedNow.flatMap { now =>
      errorHistory.get.map(_.iterator().asScala.toList).map { re =>
        histories.service_error_history(serviceParams, re, now)
      }
    }

  val service_health_check: F[Either[String, Json]] = {
    val deps_health_check: F[Json] =
      metricsPublisher.get_snapshot_history.map { queue =>
        val res = queue.lastOption
          .map(ms => retrieveHealthChecks(ms.snapshot.gauges).values)
          .fold(true)(_.forall(identity))

        Json.obj("healthy" -> Json.fromBoolean(res))
      }

    lifecyclePublisher.get_panic_history.map(_.lastOption).flatMap {
      case None      => deps_health_check.map(Right(_))
      case Some(evt) =>
        Clock[F].realTimeInstant.flatMap { now =>
          if (evt.tick.conclude.isAfter(now)) {
            val recover = Duration.between(now, evt.tick.conclude)
            s"Service panic detected. Restarting in ${durationFormatter.format(recover)}".asLeft[Json].pure[F]
          } else {
            deps_health_check.map(Right(_))
          }
        }
    }
  }

  val service_stop: F[Text.TypedTag[String]] = {
    val stopping = html(
      head(
        meta(attr("http-equiv") := "refresh", attr("content") := "3;url=/"),
        tag("title")(serviceParams.serviceName.value)),
      body(h1("Stopping Service"))
    )

    lifecyclePublisher.service_stop(StopReason.Maintenance).as(stopping)
  }
}
