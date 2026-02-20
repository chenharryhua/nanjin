package com.github.chenharryhua.nanjin.guard.observers.postgres

import cats.Applicative
import com.github.chenharryhua.nanjin.guard.config.Attribute
import com.github.chenharryhua.nanjin.guard.event.{Event, EventName, Index, MetricSnapshot}
import com.github.chenharryhua.nanjin.guard.translator.{jsonHelper, SnapshotPolyglot, Translator}
import io.circe.Json

private object JsonTranslator {
  import Event.*

  private def metrics(ss: MetricSnapshot): (String, Json) =
    "metrics" -> new SnapshotPolyglot(ss).toVanillaJson

  private def service_started(evt: ServiceStart): Json =
    Json.obj(
      "event" -> EventName.ServiceStart.snakeJson,
      Attribute(Index(evt.tick.index)).snakeJsonEntry,
      "params" -> evt.serviceParams.simpleJson,
      Attribute(evt.timestamp).snakeJsonEntry
    )

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      "event" -> EventName.ServicePanic.snakeJson,
      Attribute(Index(evt.tick.index)).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.serviceParams.servicePolicies.restart.policy).snakeJsonEntry,
      Attribute(evt.stackTrace).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.timestamp).snakeJsonEntry
    )

  private def service_stopped(evt: ServiceStop): Json =
    Json.obj(
      "event" -> EventName.ServiceStop.snakeJson,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.cause).snakeJsonEntry,
      Attribute(evt.serviceParams.servicePolicies.restart.policy).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.timestamp).snakeJsonEntry
    )

  private def metric_report(evt: MetricsReport): Json =
    Json.obj(
      "event" -> EventName.MetricsReport.snakeJson,
      Attribute(evt.index).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.took).snakeJsonEntry,
      metrics(evt.snapshot),
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.timestamp).snakeJsonEntry
    )

  private def metric_reset(evt: MetricsReset): Json =
    Json.obj(
      "event" -> EventName.MetricsReset.snakeJson,
      Attribute(evt.index).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.took).snakeJsonEntry,
      metrics(evt.snapshot),
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.timestamp).snakeJsonEntry
    )

  private def service_message(evt: ServiceMessage): Json =
    Json.obj(
      "event" -> EventName.ServiceMessage.snakeJson,
      "message" -> jsonHelper.json_service_message(evt),
      Attribute(evt.timestamp).snakeJsonEntry
    )

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(service_started)
      .withServiceStop(service_stopped)
      .withServicePanic(service_panic)
      .withMetricsReport(metric_report)
      .withMetricsReset(metric_reset)
      .withServiceMessage(service_message)

}
