package com.github.chenharryhua.nanjin.guard.observers.postgres

import cats.Applicative
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.translator.{
  interpretServiceParams,
  Attribute,
  SnapshotPolyglot,
  Translator
}
import io.circe.Json

private object JsonTranslator {
  import Event.*
  private case class Index(value: Long)

  private def service_started(evt: ServiceStart): Json =
    Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(Index(evt.tick.index)).map(_.value).snakeJsonEntry,
      "params" -> interpretServiceParams(evt.serviceParams)
    )

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(Index(evt.tick.index)).map(_.value).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.serviceParams.servicePolicies.restart.policy).map(_.show).snakeJsonEntry,
      Attribute(evt.stackTrace).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry
    )

  private def service_stopped(evt: ServiceStop): Json =
    Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.cause).snakeJsonEntry,
      Attribute(evt.serviceParams.servicePolicies.restart.policy).map(_.show).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry
    )

  private def metrics_event(evt: MetricsEvent): Json =
    Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(evt.index).map(_.show).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.took).map(_.show).snakeJsonEntry,
      Attribute(evt.snapshot).map(new SnapshotPolyglot(_).toVanillaJson).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry
    )

  private def service_message(evt: ServiceMessage): Json =
    Json.obj(
      Attribute(evt).map(_.timestamp.value).snakeJsonEntry,
      Attribute(evt.message).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.domain).snakeJsonEntry,
      Attribute(evt.correlation).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.stackTrace).snakeJsonEntry
    )

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(service_started)
      .withServiceStop(service_stopped)
      .withServicePanic(service_panic)
      .withMetricsReport(metrics_event)
      .withMetricsReset(metrics_event)
      .withServiceMessage(service_message)

}
