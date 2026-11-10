package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.syntax.show.given
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.config.{ServiceParams, Timestamp}
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsSnapshot, ReportedEvent}
import com.github.chenharryhua.nanjin.guard.event.{Active, Event, Snooze, Took}
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.retrieve
import com.github.chenharryhua.nanjin.guard.translator.{htmlColoring, Labelled, SnapshotPolyglot}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances
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

  /** The panic the service is still recovering from, if any: the most recent panic whose scheduled restart
    * (`tick.conclude`) is still in the future.
    *
    * Shared by `service_panic_history` and `service_health_check` so the `/panics` and `/health_check` routes
    * cannot disagree about whether the service is down, including at the boundary instant, where
    * `now == conclude` counts as recovered rather than recovering.
    */
  private def recovering_from(panics: Vector[Event.ServicePanic], now: Instant): Option[Event.ServicePanic] =
    panics.lastOption.filter(_.tick.conclude.isAfter(now))

  def service_panic_history(
    serviceParams: ServiceParams,
    panics: Vector[Event.ServicePanic],
    now: Timestamp): Json = {
    val active = recovering_from(panics, now.value.toInstant).isEmpty

    Json.obj(
      Labelled(serviceParams.serviceIdentity.service).snakeJsonEntry,
      Labelled(serviceParams.serviceIdentity.serviceId).snakeJsonEntry,
      "active" -> Json.fromBoolean(active),
      Labelled(Present(now.value)).map(_.json).snakeJsonEntry,
      Labelled(serviceParams.policies.restart.policy).map(_.show).snakeJsonEntry,
      Labelled(serviceParams.serviceIdentity.timeZone).snakeJsonEntry,
      Labelled(serviceParams.serviceIdentity.launchTime.upTime(now)).map(_.show).snakeJsonEntry,
      "panics" -> panics.size.asJson,
      "history" ->
        panics.reverse.map { sp =>
          Json.obj(
            "index" -> Json.fromLong(sp.tick.index),
            Labelled(Age(Duration.between(sp.timestamp.value, now.value))).map(_.json).snakeJsonEntry,
            "up_rouse_at" -> sp.tick.local(_.commence).asJson,
            Labelled(Active(sp.tick.active)).map(_.show).snakeJsonEntry,
            "panic_at" -> sp.tick.local(_.acquires).asJson,
            Labelled(Snooze(sp.tick.snooze)).map(_.show).snakeJsonEntry,
            "restart_at" -> sp.tick.local(_.conclude).asJson,
            Labelled(sp.stackTrace).snakeJsonEntry
          )
        }.asJson
    )
  }

  def service_error_history(
    serviceParams: ServiceParams,
    reportedEvents: Vector[ReportedEvent],
    now: Timestamp): Json =
    Json.obj(
      Labelled(serviceParams.serviceIdentity.service).snakeJsonEntry,
      Labelled(serviceParams.serviceIdentity.serviceId).snakeJsonEntry,
      Labelled(Present(now.value)).map(_.json).snakeJsonEntry,
      Labelled(serviceParams.serviceIdentity.timeZone).snakeJsonEntry,
      Labelled(serviceParams.serviceIdentity.launchTime.upTime(now)).map(_.show).snakeJsonEntry,
      "errors" -> reportedEvents.size.asJson,
      "history" -> reportedEvents.reverse.map { sm =>
        Json.obj(
          Labelled(sm.domain).snakeJsonEntry,
          Labelled(sm.correlation).snakeJsonEntry,
          Labelled(Age(Duration.between(sm.timestamp.value, now.value))).map(_.json).snakeJsonEntry,
          Labelled(sm.timestamp).map(_.value.toLocalDateTime).snakeJsonEntry,
          Labelled(sm.logRecord.message).snakeJsonEntry,
          Labelled(sm.logRecord.stackTrace).snakeJsonEntry
        )
      }.asJson
    )

  /** The `/health_check` verdict: `Left` (served as 503) while the service is recovering from a panic,
    * otherwise `Right` carrying the dependency verdict.
    *
    * ===What the dependency verdict actually covers===
    * `healthy` is derived from the `healthCheck` gauges of the '''most recent metrics snapshot in history''',
    * not from a live read, so it is only as current as the last snapshot and reports `true` whenever no
    * snapshot is available. Two common configurations make it vacuously `true`:
    *   - the default report policy is empty, so no periodic snapshot is ever taken; unless the service sets
    *     `withReportPolicy` or calls `agent.adhoc.report`, the history stays empty;
    *   - a snapshot history capacity of zero or less (`withHistory`) disables the history outright.
    *
    * In either case a service whose health checks are all failing still answers `healthy: true` on the
    * dependency dimension. The panic arm above is unaffected and always reflects live state. Reading the
    * gauges live instead would mean a full scrape (every gauge, on the blocking pool) per health request.
    */
  def service_health_check(
    panics: Vector[Event.ServicePanic],
    snapshots: Vector[Event.MetricsSnapshot],
    now: Instant): Either[String, Json] = {
    val deps_health_check: Json = {
      val res = snapshots.lastOption
        .map(ms => retrieve.healthCheck(ms.snapshot.gauges).values)
        .fold(true)(_.forall(identity))

      Json.obj("healthy" -> Json.fromBoolean(res))
    }

    recovering_from(panics, now) match {
      case None      => Right(deps_health_check)
      case Some(evt) =>
        val recover = Duration.between(now, evt.tick.conclude)
        Left(s"Service panic detected. Restarting in ${defaultFormatter.format(recover)}")
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
    val service_name = Labelled(serviceParams.serviceIdentity.service).textEntry
    val policy = Labelled(serviceParams.policies.report).textEntry
    val timezone = Labelled(serviceParams.serviceIdentity.timeZone).textEntry
    val uptime = Labelled(serviceParams.serviceIdentity.launchTime.upTime(Timestamp(now))).textEntry
    val present = Labelled(Present(now)).map(_.text).textEntry
    took.fold(
      table(
        tr(th(service_name.tag), th(policy.tag), th(timezone.tag), th(uptime.tag), th(present.tag)),
        tr(td(service_name.text), td(policy.text), td(timezone.text), td(uptime.text), td(present.text))
      )
    ) { tk =>
      val spend = Labelled(Took(tk)).textEntry
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

  def snapshot_to_yaml_html(title: String, serviceParams: ServiceParams)(
    ms: MetricsSnapshot): Text.TypedTag[String] = {
    val yaml = new SnapshotPolyglot(ms.snapshot).toYaml
    html(
      html_header(s"$title-${ms.serviceIdentity.service.value}"),
      body(div(table_title_section(serviceParams, ms.timestamp.value, Some(ms.took.value)), pre(yaml)))
    )
  }

  def metrics_history(
    serviceParams: ServiceParams,
    metricsSnapshots: Vector[Event.MetricsSnapshot],
    now: ZonedDateTime): Text.TypedTag[String] = {

    val list = metricsSnapshots.reverse.map { mr =>
      val took = Labelled(mr.took).textEntry
      val idx = Labelled(mr.index).textEntry
      val timestamp = Labelled(mr.timestamp).map(_.value.toLocalDateTime.show).textEntry
      div(
        table(
          tr(th(style := htmlColoring(mr))(idx.tag), th(timestamp.tag), th(took.tag)),
          tr(td(idx.text), td(timestamp.text), td(took.text))
        ),
        pre(new SnapshotPolyglot(mr.snapshot).toYaml)
      )
    }

    val histories =
      div(table_title_section(serviceParams, now, None), h3("Metrics History"), list)

    html(html_header(s"History-${serviceParams.serviceIdentity.service.value}"), body(div(histories)))
  }
}
