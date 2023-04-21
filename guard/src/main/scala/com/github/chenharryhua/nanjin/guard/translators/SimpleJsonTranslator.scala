package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import io.circe.syntax.*

private object SimpleJsonTranslator {
  import NJEvent.*

  private def timestamp(evt: NJEvent): (String, Json) =
    "timestamp" -> evt.timestamp.asJson
  private def serviceId(evt: NJEvent): (String, Json) =
    "serviceId" -> evt.serviceParams.serviceId.asJson
  private def serviceName(evt: NJEvent): (String, Json) =
    "serviceName" -> Json.fromString(evt.serviceParams.serviceName)

  private def name(mn: MetricName): (String, Json) =
    "name" -> Json.fromString(mn.value)
  private def digest(mn: MetricName): (String, Json) =
    "digest" -> Json.fromString(mn.digest)
  private def measurement(mn: MetricName): (String, Json) =
    "measurement" -> Json.fromString(mn.measurement)
  private def actionId(evt: ActionEvent): (String, Json) =
    "id" -> Json.fromString(evt.actionId)
  private def traceId(evt: ActionEvent): (String, Json) =
    "traceId" -> evt.actionInfo.traceId.asJson
  private def took(evt: ActionResultEvent): (String, Json) =
    "took" -> evt.took.asJson
  private def stackTrace(err: NJError): (String, Json) =
    "stackTrace" -> Json.fromString(err.stackTrace)

  private def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc           => "index" -> Json.Null
    case MetricIndex.Periodic(index) => "index" -> Json.fromInt(index)
  }

  private def policy(evt: NJEvent): (String, Json) =
    "policy" -> Json.fromString(evt.serviceParams.restartPolicy)

  private def metrics(ss: MetricSnapshot): (String, Json) =
    "metrics" -> new SnapshotJson(ss).toVanillaJson

  private def serviceStarted(evt: ServiceStart): Json =
    Json.obj(
      "event" -> EventName.ServiceStart.camelJson,
      "params" -> evt.serviceParams.asJson,
      timestamp(evt))

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
      ("exitCode", Json.fromInt(evt.cause.exitCode)),
      ("cause", Json.fromString(evt.cause.show)),
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
      "level" -> evt.alertLevel.asJson,
      name(evt.metricName),
      "message" -> evt.message,
      digest(evt.metricName),
      serviceId(evt),
      timestamp(evt)
    )

  private def actionStart(evt: ActionStart): Json =
    Json.obj(
      "event" -> EventName.ActionStart.camelJson,
      name(evt.metricId.metricName),
      measurement(evt.actionParams.metricId.metricName),
      traceId(evt),
      digest(evt.metricId.metricName),
      actionId(evt),
      "input" -> evt.json,
      serviceId(evt),
      timestamp(evt)
    )

  private def actionRetrying(evt: ActionRetry): Json =
    Json.obj(
      "event" -> EventName.ActionRetry.camelJson,
      name(evt.metricId.metricName),
      measurement(evt.actionParams.metricId.metricName),
      traceId(evt),
      ("cause", Json.fromString(evt.error.message)),
      digest(evt.metricId.metricName),
      actionId(evt),
      serviceId(evt),
      timestamp(evt)
    )

  private def actionFail(evt: ActionFail): Json =
    Json.obj(
      "event" -> EventName.ActionFail.camelJson,
      name(evt.metricId.metricName),
      measurement(evt.actionParams.metricId.metricName),
      took(evt),
      traceId(evt),
      "input" -> evt.json, // align with slack
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
      measurement(evt.actionParams.metricId.metricName),
      took(evt),
      traceId(evt),
      "result" -> evt.json, // align with slack
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
