package com.github.chenharryhua.nanjin.guard.translator

import cats.Applicative
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.event.{Active, Index, Snooze}
import io.circe.Json

object PrettyJsonTranslator {

  // events handlers
  private def service_start(evt: ServiceStart): Json =
    Json.obj(
      Attribute(evt).map(_.tick.index).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.upTime.show).snakeJsonEntry,
      Attribute(Snooze(evt.tick.snooze)).map(_.show).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      "params" -> interpretServiceParams(evt.serviceParams)
    )

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      Attribute(evt).map(_.tick.index).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(Active(evt.tick.active)).map(_.show).snakeJsonEntry,
      Attribute(Snooze(evt.tick.snooze)).map(_.show).snakeJsonEntry,
      Attribute(evt.serviceParams.servicePolicies.restart.policy).map(_.show).snakeJsonEntry,
      Attribute(evt.upTime.show).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.stackTrace).snakeJsonEntry
    )

  private def service_stop(evt: ServiceStop): Json =
    Json.obj(
      Attribute(evt).map(_.upTime.show).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.serviceParams.servicePolicies.restart.policy).map(_.show).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.cause).snakeJsonEntry
    )

  private def metrics_event(evt: MetricsEvent): Json =
    Json.obj(
      Attribute(evt).map {
        _.index match {
          case ac @ Index.Adhoc(_)  => Json.fromString(s"${evt.kind.show}-${ac.productPrefix}")
          case Index.Periodic(tick) => Json.fromString(s"${evt.kind.show}-${tick.index}")
        }
      }.snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.took).map(_.show).snakeJsonEntry,
      Attribute(evt.kind.policy).map(_.show).snakeJsonEntry,
      Attribute(evt.upTime.show).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.snapshot).map(new SnapshotPolyglot(_).toPrettyJson).snakeJsonEntry
    )

  private def reported_event(evt: ReportedEvent): Json =
    Json
      .obj(
        Attribute(evt).map(_.level.entryName).snakeJsonEntry,
        Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
        Attribute(evt.domain).snakeJsonEntry,
        Attribute(evt.correlation).snakeJsonEntry,
        Attribute(evt.upTime.show).snakeJsonEntry,
        Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
        Attribute(evt.message).snakeJsonEntry,
        Attribute(evt.stackTrace).snakeJsonEntry
      )
      .dropNullValues

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(service_start)
      .withServiceStop(service_stop)
      .withServicePanic(service_panic)
      .withMetricsEvent(metrics_event)
      .withReportedEvent(reported_event)
}
