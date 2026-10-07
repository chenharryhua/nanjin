package com.github.chenharryhua.nanjin.guard.translator

import cats.Applicative
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.event.{Active, Snooze}
import io.circe.Json

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
