package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJEvent}
import io.circe.Json

private object SimpleJsonTranslator {
  import NJEvent.*
  import jsonInterpreter.*

  private def metrics(ss: MetricSnapshot): (String, Json) =
    "metrics" -> new SnapshotJson(ss).toVanillaJson

  def name(mn: MetricName): (String, Json)   = "name" -> Json.fromString(mn.value)
  def digest(mn: MetricName): (String, Json) = "digest" -> Json.fromString(mn.digest)

  private def serviceStarted(evt: ServiceStart): Json =
    Json.obj("event" -> EventName.ServiceStart.camelJson, serviceParams(evt.serviceParams), timestamp(evt))

  private def servicePanic(evt: ServicePanic): Json =
    Json.obj(
      "event" -> EventName.ServicePanic.camelJson,
      serviceName(evt),
      policy(evt),
      stackTrace(evt.error),
      serviceId(evt),
      timestamp(evt)
    )

  private def serviceStopped(evt: ServiceStop): Json =
    Json.obj(
      "event" -> EventName.ServiceStop.camelJson,
      serviceName(evt),
      exitCade(evt.cause),
      exitCause(evt.cause),
      policy(evt),
      serviceId(evt),
      timestamp(evt)
    )

  private def metricReport(evt: MetricReport): Json =
    Json.obj(
      "event" -> EventName.MetricReport.camelJson,
      metricIndex(evt.index),
      serviceName(evt),
      metrics(evt.snapshot),
      serviceId(evt),
      timestamp(evt)
    )

  private def metricReset(evt: MetricReset): Json =
    Json.obj(
      "event" -> EventName.MetricReset.camelJson,
      metricIndex(evt.index),
      serviceName(evt),
      metrics(evt.snapshot),
      serviceId(evt),
      timestamp(evt)
    )

  private def serviceAlert(evt: ServiceAlert): Json =
    Json.obj(
      "event" -> EventName.ServiceAlert.camelJson,
      alertMessage(evt),
      name(evt.metricName),
      digest(evt.metricName),
      serviceId(evt),
      timestamp(evt)
    )

  private def actionStart(evt: ActionStart): Json =
    Json.obj(
      "event" -> EventName.ActionStart.camelJson,
      name(evt.metricId.metricName),
      importance(evt),
      publishStrategy(evt),
      measurement(evt.actionParams.metricId.metricName),
      traceId(evt),
      digest(evt.metricId.metricName),
      actionId(evt),
      notes(evt.notes),
      serviceId(evt),
      timestamp(evt)
    )

  private def actionRetrying(evt: ActionRetry): Json =
    Json.obj(
      "event" -> EventName.ActionRetry.camelJson,
      name(evt.metricId.metricName),
      importance(evt),
      publishStrategy(evt),
      measurement(evt.actionParams.metricId.metricName),
      traceId(evt),
      errCause(evt.error),
      digest(evt.metricId.metricName),
      actionId(evt),
      serviceId(evt),
      timestamp(evt)
    )

  private def actionFail(evt: ActionFail): Json =
    Json.obj(
      "event" -> EventName.ActionFail.camelJson,
      name(evt.metricId.metricName),
      importance(evt),
      publishStrategy(evt),
      measurement(evt.actionParams.metricId.metricName),
      took(evt),
      traceId(evt),
      notes(evt.notes),
      stackTrace(evt.error),
      digest(evt.metricId.metricName),
      actionId(evt),
      serviceId(evt),
      timestamp(evt)
    )

  private def actionComplete(evt: ActionComplete): Json =
    Json.obj(
      "event" -> EventName.ActionComplete.camelJson,
      name(evt.metricId.metricName),
      importance(evt),
      publishStrategy(evt),
      measurement(evt.actionParams.metricId.metricName),
      took(evt),
      traceId(evt),
      notes(evt.notes),
      digest(evt.metricId.metricName),
      actionId(evt),
      serviceId(evt),
      timestamp(evt)
    )

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(serviceStarted)
      .withServiceStop(serviceStopped)
      .withServicePanic(servicePanic)
      .withMetricReport(metricReport)
      .withMetricReset(metricReset)
      .withServiceAlert(serviceAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFail)
      .withActionComplete(actionComplete)

}
