package com.github.chenharryhua.nanjin.guard.observers.postgres

import cats.Applicative
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJEvent}
import com.github.chenharryhua.nanjin.guard.translator.{jsonHelper, EventName, SnapshotPolyglot, Translator}
import io.circe.Json
import io.circe.syntax.EncoderOps

import java.time.Duration

private object JsonTranslator {
  import NJEvent.*
  import jsonHelper.*

  private def took(dur: Duration): (String, Json) = "took" -> dur.asJson

  private def metrics(ss: MetricSnapshot): (String, Json) =
    "metrics" -> new SnapshotPolyglot(ss).toVanillaJson

  private def service_started(evt: ServiceStart): Json =
    Json.obj(
      "event" -> EventName.ServiceStart.camelJson,
      index(evt.tick),
      serviceParams(evt.serviceParams),
      timestamp(evt))

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      "event" -> EventName.ServicePanic.camelJson,
      index(evt.tick),
      serviceName(evt),
      policy(evt.serviceParams.servicePolicies.restart),
      stack(evt.error),
      serviceId(evt),
      timestamp(evt)
    )

  private def service_stopped(evt: ServiceStop): Json =
    Json.obj(
      "event" -> EventName.ServiceStop.camelJson,
      serviceName(evt),
      exitCode(evt.cause),
      exitCause(evt.cause),
      policy(evt.serviceParams.servicePolicies.restart),
      serviceId(evt),
      timestamp(evt)
    )

  private def metric_report(evt: MetricReport): Json =
    Json.obj(
      "event" -> EventName.MetricReport.camelJson,
      metricIndex(evt.index),
      serviceName(evt),
      took(evt.took),
      metrics(evt.snapshot),
      serviceId(evt),
      timestamp(evt)
    )

  private def metric_reset(evt: MetricReset): Json =
    Json.obj(
      "event" -> EventName.MetricReset.camelJson,
      metricIndex(evt.index),
      serviceName(evt),
      took(evt.took),
      metrics(evt.snapshot),
      serviceId(evt),
      timestamp(evt)
    )

  private def service_alert(evt: ServiceMessage): Json =
    Json.obj(
      "event" -> EventName.ServiceAlert.camelJson,
      alertMessage(evt),
      metricName(evt.metricName),
      metricDigest(evt.metricName),
      metricMeasurement(evt.metricName),
      serviceId(evt),
      timestamp(evt)
    )

  private def action_start(evt: ActionStart): Json =
    Json.obj(
      "event" -> EventName.ActionStart.camelJson,
      metricName(evt.actionParams.metricName),
      metricDigest(evt.actionParams.metricName),
      metricMeasurement(evt.actionParams.metricName),
      config(evt),
      notes(evt.notes),
      serviceId(evt),
      timestamp(evt)
    )

  private def action_retrying(evt: ActionRetry): Json =
    Json.obj(
      "event" -> EventName.ActionRetry.camelJson,
      metricName(evt.actionParams.metricName),
      metricDigest(evt.actionParams.metricName),
      metricMeasurement(evt.actionParams.metricName),
      config(evt),
      errorCause(evt.error),
      serviceId(evt),
      timestamp(evt)
    )

  private def action_fail(evt: ActionFail): Json =
    Json.obj(
      "event" -> EventName.ActionFail.camelJson,
      metricName(evt.actionParams.metricName),
      metricDigest(evt.actionParams.metricName),
      metricMeasurement(evt.actionParams.metricName),
      config(evt),
      notes(evt.notes),
      stack(evt.error),
      serviceId(evt),
      timestamp(evt)
    )

  private def action_done(evt: ActionDone): Json =
    Json.obj(
      "event" -> EventName.ActionDone.camelJson,
      metricName(evt.actionParams.metricName),
      metricDigest(evt.actionParams.metricName),
      metricMeasurement(evt.actionParams.metricName),
      config(evt),
      took(evt.took),
      notes(evt.notes),
      serviceId(evt),
      timestamp(evt)
    )

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(service_started)
      .withServiceStop(service_stopped)
      .withServicePanic(service_panic)
      .withMetricReport(metric_report)
      .withMetricReset(metric_reset)
      .withServiceMessage(service_alert)
      .withActionStart(action_start)
      .withActionRetry(action_retrying)
      .withActionFail(action_fail)
      .withActionDone(action_done)

}
