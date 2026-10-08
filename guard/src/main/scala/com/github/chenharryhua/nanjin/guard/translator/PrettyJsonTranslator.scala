package com.github.chenharryhua.nanjin.guard.translator

import cats.Applicative
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.event.{Active, Snooze}
import io.circe.Json

/** Translates events into the "pretty", display-oriented JSON shape.
  *
  * "Pretty" is the same distinction `SnapshotPolyglot` draws: the metrics snapshot is rendered with
  * `toPrettyJson` (unit-formatted string values, null gauges dropped) rather than `toVanillaJson` (the
  * encoder-derived form kept for persistence). Across the rest of the event, values are likewise rendered
  * through their `Show` instances and keyed in `snake_case`, and `reportedEvent` drops null fields. The
  * result favors human readability over a canonical, round-trippable encoding.
  *
  * This is the shared event-JSON shape behind the observers that emit JSON (e.g. the otel4s observer, where
  * `JsonToAnyValue` adapts it into a structured OpenTelemetry log body). Reusing one translator keeps those
  * outputs consistent with each other and with the normal application logs: a change here moves every
  * consumer together.
  */
object PrettyJsonTranslator {

  // events handlers
  private def service_start(evt: ServiceStart): Json =
    Json.obj(
      Labelled(evt).map(_.tick.index).snakeJsonEntry,
      Labelled(evt.serviceIdentity.service).snakeJsonEntry,
      Labelled(evt.upTime).map(_.show).snakeJsonEntry,
      Labelled(Snooze(evt.tick.snooze)).map(_.show).snakeJsonEntry,
      Labelled(evt.serviceIdentity.serviceId).snakeJsonEntry,
      Labelled(evt.brief).snakeJsonEntry
    )

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      Labelled(evt).map(_.tick.index).snakeJsonEntry,
      Labelled(evt.serviceIdentity.service).snakeJsonEntry,
      Labelled(Active(evt.tick.active)).map(_.show).snakeJsonEntry,
      Labelled(Snooze(evt.tick.snooze)).map(_.show).snakeJsonEntry,
      Labelled(evt.upTime).map(_.show).snakeJsonEntry,
      Labelled(evt.serviceIdentity.serviceId).snakeJsonEntry,
      Labelled(evt.stackTrace).snakeJsonEntry
    )

  private def service_stop(evt: ServiceStop): Json =
    Json.obj(
      Labelled(evt).map(_.cause).snakeJsonEntry,
      Labelled(evt.serviceIdentity.service).snakeJsonEntry,
      Labelled(evt.serviceIdentity.serviceId).snakeJsonEntry,
      Labelled(evt.upTime).map(_.show).snakeJsonEntry
    )

  private def metrics_snapshot(evt: MetricsSnapshot): Json =
    Json.obj(
      Labelled(evt).map(_.index.show).snakeJsonEntry,
      Labelled(evt.serviceIdentity.service).snakeJsonEntry,
      Labelled(evt.took).map(_.show).snakeJsonEntry,
      Labelled(evt.upTime).map(_.show).snakeJsonEntry,
      Labelled(evt.serviceIdentity.serviceId).snakeJsonEntry,
      Labelled(evt.snapshot).map(new SnapshotPolyglot(_).toPrettyJson).snakeJsonEntry
    )

  private def reported_event(evt: ReportedEvent): Json =
    Json
      .obj(
        Labelled(evt.correlation).snakeJsonEntry,
        Labelled(evt.domain).snakeJsonEntry,
        Labelled(evt).map(_.logRecord.level.show).snakeJsonEntry,
        Labelled(evt.serviceIdentity.service).snakeJsonEntry,
        Labelled(evt.serviceIdentity.serviceId).snakeJsonEntry,
        Labelled(evt.upTime).map(_.show).snakeJsonEntry,
        Labelled(evt.logRecord.message).snakeJsonEntry,
        Labelled(evt.logRecord.stackTrace).snakeJsonEntry
      )
      .dropNullValues

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(service_start)
      .withServiceStop(service_stop)
      .withServicePanic(service_panic)
      .withMetricsSnapshot(metrics_snapshot)
      .withReportedEvent(reported_event)
}
