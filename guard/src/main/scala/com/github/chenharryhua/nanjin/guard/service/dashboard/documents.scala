package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.syntax.show.given
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsSnapshot, ReportedEvent}
import com.github.chenharryhua.nanjin.guard.event.{
  retrieveHealthChecks,
  Active,
  Event,
  Snooze,
  Timestamp,
  Took
}
import com.github.chenharryhua.nanjin.guard.translator.{htmlColoring, Attribute, SnapshotPolyglot}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.typelevel.cats.time.localtimeInstances
import scalatags.Text
import scalatags.Text.all.*

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, ZonedDateTime}

private object documents {
  private case class Present(value: ZonedDateTime) {
    val text: String = value.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show
    val json: Json = text.asJson
  }
  private case class Age(value: Duration) {
    val json: Json = defaultFormatter.format(value).asJson
  }

  /*
   * Json
   */

  def service_panic_history(
    serviceParams: ServiceParams,
    panics: Vector[Event.ServicePanic],
    now: ZonedDateTime): Json = {
    val active = panics.lastOption.map(_.tick.conclude).forall(_.isBefore(now.toInstant))

    Json.obj(
      Attribute(serviceParams.serviceName).snakeJsonEntry,
      Attribute(serviceParams.serviceId).snakeJsonEntry,
      "active" -> Json.fromBoolean(active),
      Attribute(Present(now)).map(_.json).snakeJsonEntry,
      Attribute(serviceParams.servicePolicies.restart.policy).map(_.show).snakeJsonEntry,
      Attribute(serviceParams.timeZone).snakeJsonEntry,
      Attribute(serviceParams.upTime(now)).map(_.show).snakeJsonEntry,
      "panics" -> panics.size.asJson,
      "history" ->
        panics.reverse.map { sp =>
          Json.obj(
            "index" -> Json.fromLong(sp.tick.index),
            Attribute(Age(Duration.between(sp.timestamp.value, now))).map(_.json).snakeJsonEntry,
            "up_rouse_at" -> sp.tick.local(_.commence).asJson,
            Attribute(Active(sp.tick.active)).map(_.show).snakeJsonEntry,
            Attribute(Timestamp(sp.tick.zoned(_.acquires)))
              .map(_.value.toLocalDateTime)
              .snakeJsonEntry,
            Attribute(Snooze(sp.tick.snooze)).map(_.show).snakeJsonEntry,
            "restart_at" -> sp.tick.local(_.conclude).asJson,
            Attribute(sp.stackTrace).snakeJsonEntry
          )
        }.asJson
    )
  }

  def service_error_history(
    serviceParams: ServiceParams,
    reportedEvents: Vector[ReportedEvent],
    now: ZonedDateTime): Json =
    Json.obj(
      Attribute(serviceParams.serviceName).snakeJsonEntry,
      Attribute(serviceParams.serviceId).snakeJsonEntry,
      Attribute(Present(now)).map(_.json).snakeJsonEntry,
      Attribute(serviceParams.timeZone).snakeJsonEntry,
      Attribute(serviceParams.upTime(now)).map(_.show).snakeJsonEntry,
      "errors" -> reportedEvents.size.asJson,
      "history" -> reportedEvents.reverse.map { sm =>
        Json.obj(
          Attribute(sm.domain).snakeJsonEntry,
          Attribute(sm.correlation).snakeJsonEntry,
          Attribute(Age(Duration.between(sm.timestamp.value, now))).map(_.json).snakeJsonEntry,
          Attribute(sm.timestamp).map(_.value.toLocalDateTime).snakeJsonEntry,
          Attribute(sm.message).snakeJsonEntry,
          Attribute(sm.stackTrace).snakeJsonEntry
        )
      }.asJson
    )

  def service_health_check(
    panics: Vector[Event.ServicePanic],
    snapshots: Vector[Event.MetricsSnapshot],
    now: Instant): Either[String, Json] = {
    val deps_health_check: Json = {
      val res = snapshots.lastOption
        .map(ms => retrieveHealthChecks(ms.snapshot.gauges).values)
        .fold(true)(_.forall(identity))

      Json.obj("healthy" -> Json.fromBoolean(res))
    }

    panics.lastOption match {
      case None      => Right(deps_health_check)
      case Some(evt) =>
        if evt.tick.conclude.isAfter(now) then
          val recover = Duration.between(now, evt.tick.conclude)
          Left(s"Service panic detected. Restarting in ${defaultFormatter.format(recover)}")
        else Right(deps_health_check)
    }
  }

  /*
   * Html
   */

  private def html_header(title: String): Text.TypedTag[String] =
    head(
      tag("style")("""
        td, th {text-align: left; padding: 2px; border: 1px solid;}
        table {
          border-collapse: collapse;
          width: 70%;
        }
      """),
      tag("title")(title)
    )

  private def table_title_section(
    serviceParams: ServiceParams,
    now: ZonedDateTime,
    took: Option[Duration]): Text.TypedTag[String] = {
    val service_name = Attribute(serviceParams.serviceName).textEntry
    val policy = Attribute(serviceParams.servicePolicies.report.policy).textEntry
    val timezone = Attribute(serviceParams.timeZone).textEntry
    val uptime = Attribute(serviceParams.upTime(now)).textEntry
    val present = Attribute(Present(now)).map(_.text).textEntry
    took.fold(
      table(
        tr(th(service_name.tag), th(policy.tag), th(timezone.tag), th(uptime.tag), th(present.tag)),
        tr(td(service_name.text), td(policy.text), td(timezone.text), td(uptime.text), td(present.text))
      )
    ) { tk =>
      val spend = Attribute(Took(tk)).textEntry
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
  }

  def snapshot_to_yaml_html(title: String)(ms: MetricsSnapshot): Text.TypedTag[String] = {
    val yaml = new SnapshotPolyglot(ms.snapshot).toYaml
    html(
      html_header(s"$title-${ms.serviceParams.serviceName.value}"),
      body(div(table_title_section(ms.serviceParams, ms.timestamp.value, Some(ms.took.value)), pre(yaml)))
    )
  }

  def metrics_history(
    serviceParams: ServiceParams,
    metricsSnapshots: Vector[Event.MetricsSnapshot],
    now: ZonedDateTime): Text.TypedTag[String] = {

    val list = metricsSnapshots.reverse.map { mr =>
      val took = Attribute(mr.took).textEntry
      val label = Attribute(mr.label).textEntry
      val timestamp = Attribute(mr.timestamp).textEntry
      div(
        table(
          tr(th(style := htmlColoring(mr))(label.tag), th(timestamp.tag), th(took.tag)),
          tr(td(label.text), td(timestamp.text), td(took.text))
        ),
        pre(new SnapshotPolyglot(mr.snapshot).toYaml)
      )
    }

    val histories =
      div(table_title_section(serviceParams, now, None), h3("Metrics History"), list)

    html(html_header(s"History-${serviceParams.serviceName.value}"), body(div(histories)))
  }
}
