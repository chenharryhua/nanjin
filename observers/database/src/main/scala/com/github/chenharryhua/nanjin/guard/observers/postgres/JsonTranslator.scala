package com.github.chenharryhua.nanjin.guard.observers.postgres

import cats.Applicative
import com.github.chenharryhua.nanjin.guard.event.{Event, EventName, MetricSnapshot}
import com.github.chenharryhua.nanjin.guard.translator.{jsonHelper, SnapshotPolyglot, Translator}
import io.circe.Json
import io.circe.syntax.EncoderOps

import java.time.Duration

private object JsonTranslator {
  import Event.*

  private def took(dur: Duration): (String, Json) = "took" -> dur.asJson

  private def metrics(ss: MetricSnapshot): (String, Json) =
    "metrics" -> new SnapshotPolyglot(ss).toVanillaJson

  private def service_started(evt: ServiceStart): Json =
    Json.obj(
      "event" -> EventName.ServiceStart.snakeJson,
      jsonHelper.index(evt.tick),
      jsonHelper.service_params(evt.serviceParams),
      jsonHelper.timestamp(evt))

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      "event" -> EventName.ServicePanic.snakeJson,
      jsonHelper.index(evt.tick),
      jsonHelper.service_name(evt.serviceParams),
      jsonHelper.policy(evt.serviceParams.servicePolicies.restart),
      jsonHelper.stack(evt.error),
      jsonHelper.service_id(evt.serviceParams),
      jsonHelper.timestamp(evt)
    )

  private def service_stopped(evt: ServiceStop): Json =
    Json.obj(
      "event" -> EventName.ServiceStop.snakeJson,
      jsonHelper.service_name(evt.serviceParams),
      jsonHelper.exit_code(evt.cause),
      jsonHelper.exit_cause(evt.cause),
      jsonHelper.policy(evt.serviceParams.servicePolicies.restart),
      jsonHelper.service_id(evt.serviceParams),
      jsonHelper.timestamp(evt)
    )

  private def metric_report(evt: MetricReport): Json =
    Json.obj(
      "event" -> EventName.MetricReport.snakeJson,
      jsonHelper.metric_index(evt.index),
      jsonHelper.service_name(evt.serviceParams),
      took(evt.took),
      metrics(evt.snapshot),
      jsonHelper.service_id(evt.serviceParams),
      jsonHelper.timestamp(evt)
    )

  private def metric_reset(evt: MetricReset): Json =
    Json.obj(
      "event" -> EventName.MetricReset.snakeJson,
      jsonHelper.metric_index(evt.index),
      jsonHelper.service_name(evt.serviceParams),
      took(evt.took),
      metrics(evt.snapshot),
      jsonHelper.service_id(evt.serviceParams),
      jsonHelper.timestamp(evt)
    )

  private def service_message(evt: ServiceMessage): Json =
    Json.obj(
      "event" -> EventName.ServiceMessage.snakeJson,
      "message" -> jsonHelper.json_service_message(evt),
      jsonHelper.timestamp(evt)
    )

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(service_started)
      .withServiceStop(service_stopped)
      .withServicePanic(service_panic)
      .withMetricReport(metric_report)
      .withMetricReset(metric_reset)
      .withServiceMessage(service_message)

}
