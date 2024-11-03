package com.github.chenharryhua.nanjin.guard.observers.postgres

import cats.Applicative
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJEvent}
import com.github.chenharryhua.nanjin.guard.translator.{jsonHelper, EventName, SnapshotPolyglot, Translator}
import io.circe.Json
import io.circe.syntax.EncoderOps

import java.time.Duration

private object JsonTranslator {
  import NJEvent.*

  private def took(dur: Duration): (String, Json) = "took" -> dur.asJson

  private def metrics(ss: MetricSnapshot): (String, Json) =
    "metrics" -> new SnapshotPolyglot(ss).toVanillaJson

  private def service_started(evt: ServiceStart): Json =
    Json.obj(
      "event" -> EventName.ServiceStart.camelJson,
      jsonHelper.index(evt.tick),
      jsonHelper.serviceParams(evt.serviceParams),
      jsonHelper.timestamp(evt))

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      "event" -> EventName.ServicePanic.camelJson,
      jsonHelper.index(evt.tick),
      jsonHelper.serviceName(evt.serviceParams),
      jsonHelper.policy(evt.serviceParams.servicePolicies.restart),
      jsonHelper.stack(evt.error),
      jsonHelper.serviceId(evt.serviceParams),
      jsonHelper.timestamp(evt)
    )

  private def service_stopped(evt: ServiceStop): Json =
    Json.obj(
      "event" -> EventName.ServiceStop.camelJson,
      jsonHelper.serviceName(evt.serviceParams),
      jsonHelper.exitCode(evt.cause),
      jsonHelper.exitCause(evt.cause),
      jsonHelper.policy(evt.serviceParams.servicePolicies.restart),
      jsonHelper.serviceId(evt.serviceParams),
      jsonHelper.timestamp(evt)
    )

  private def metric_report(evt: MetricReport): Json =
    Json.obj(
      "event" -> EventName.MetricReport.camelJson,
      jsonHelper.metricIndex(evt.index),
      jsonHelper.serviceName(evt.serviceParams),
      took(evt.took),
      metrics(evt.snapshot),
      jsonHelper.serviceId(evt.serviceParams),
      jsonHelper.timestamp(evt)
    )

  private def metric_reset(evt: MetricReset): Json =
    Json.obj(
      "event" -> EventName.MetricReset.camelJson,
      jsonHelper.metricIndex(evt.index),
      jsonHelper.serviceName(evt.serviceParams),
      took(evt.took),
      metrics(evt.snapshot),
      jsonHelper.serviceId(evt.serviceParams),
      jsonHelper.timestamp(evt)
    )

  private def service_message(evt: ServiceMessage): Json =
    Json.obj(
      "event" -> EventName.ServiceMessage.camelJson,
      "message" -> jsonHelper.jsonServiceMessage(evt),
      jsonHelper.serviceId(evt.serviceParams),
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
