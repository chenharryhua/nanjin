package com.github.chenharryhua.nanjin.guard.observers.postgres

import cats.Applicative
import com.github.chenharryhua.nanjin.guard.config.Attribute
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.translator.{SnapshotPolyglot, Translator}
import io.circe.Json

private object JsonTranslator {
  import Event.*
  private case class Index(value: Long)

  private def service_started(evt: ServiceStart): Json =
    Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(Index(evt.tick.index)).map(_.value).snakeJsonEntry,
      "params" -> evt.serviceParams.simpleJson
    )

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(Index(evt.tick.index)).map(_.value).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.serviceParams.servicePolicies.restart.policy).snakeJsonEntry,
      Attribute(evt.stackTrace).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry
    )

  private def service_stopped(evt: ServiceStop): Json =
    Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.cause).snakeJsonEntry,
      Attribute(evt.serviceParams.servicePolicies.restart.policy).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry
    )

  private def metric_report(evt: MetricsReport): Json =
    Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(evt.index).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.took).snakeJsonEntry,
      Attribute(evt.snapshot).map(new SnapshotPolyglot(_).toVanillaJson).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry
    )

  private def metric_reset(evt: MetricsReset): Json =
    Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(evt.index).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.took).snakeJsonEntry,
      Attribute(evt.snapshot).map(new SnapshotPolyglot(_).toVanillaJson).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry
    )

  private def service_message(evt: ServiceMessage): Json = {
    val json = Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(evt.message).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.domain).snakeJsonEntry,
      Attribute(evt.correlation).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry
    )
    evt.stackTrace.fold(json)(st => Json.obj(Attribute(st).snakeJsonEntry).deepMerge(json))
  }

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
