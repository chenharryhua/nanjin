package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.config.{Importance, MetricName}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import io.circe.syntax.*

private object SimpleJsonTranslator {
  import NJEvent.*

  private def timestamp(evt: NJEvent): (String, Json) = "timestamp" -> evt.timestamp.asJson
  private def serviceId(evt: NJEvent): (String, Json) = "service_id" -> evt.serviceId.asJson
  private def serviceName(evt: NJEvent): (String, Json) =
    ("service_name", Json.fromString(evt.serviceName.value))

  private def name(id: MetricName): (String, Json)   = "name" -> Json.fromString(id.value)
  private def digest(id: MetricName): (String, Json) = "digest" -> Json.fromString(id.digest.value)
  private def measurement(id: MetricName): (String, Json) =
    "measurement" -> Json.fromString(id.measurement.value)
  private def actionId(evt: ActionEvent): (String, Json)   = "id" -> Json.fromString(evt.actionId)
  private def traceInfo(evt: ActionEvent): (String, Json)  = "trace_info" -> evt.actionInfo.traceInfo.asJson
  private def importance(imp: Importance): (String, Json)  = "importance" -> imp.asJson
  private def took(evt: ActionResultEvent): (String, Json) = "took" -> evt.took.asJson

  private def stackTrace(err: NJError): (String, Json) = "stack_trace" -> Json.fromString(err.stackTrace)

  private def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc           => "index" -> Json.Null
    case MetricIndex.Periodic(index) => "index" -> Json.fromInt(index)
  }

  private def policy(evt: ServiceEvent): (String, Json) = "policy" -> evt.serviceParams.restartPolicy.asJson

  private def metrics(ss: MetricSnapshot): (String, Json) =
    "metrics" -> new SnapshotJson(ss).toVanillaJson

  private def serviceStarted(evt: ServiceStart): Json =
    Json.obj("event" -> "service_start".asJson, "params" -> evt.serviceParams.asJson, timestamp(evt))

  private def servicePanic(evt: ServicePanic): Json =
    Json.obj(
      "event" -> "service_panic".asJson,
      serviceName(evt),
      policy(evt),
      stackTrace(evt.error),
      serviceId(evt),
      timestamp(evt)
    )

  private def serviceStopped(evt: ServiceStop): Json =
    Json.obj(
      "event" -> "service_stop".asJson,
      serviceName(evt),
      ("exit_code", Json.fromInt(evt.cause.exitCode)),
      ("cause", Json.fromString(evt.cause.show)),
      policy(evt),
      serviceId(evt),
      timestamp(evt)
    )

  private def metricReport(evt: MetricReport): Json =
    Json.obj(
      "event" -> "metric_report".asJson,
      metricIndex(evt.index),
      serviceName(evt),
      metrics(evt.snapshot),
      serviceId(evt),
      timestamp(evt)
    )

  private def metricReset(evt: MetricReset): Json =
    Json.obj(
      "event" -> "metric_reset".asJson,
      metricIndex(evt.index),
      serviceName(evt),
      metrics(evt.snapshot),
      serviceId(evt),
      timestamp(evt)
    )

  private def instantAlert(evt: InstantAlert): Json =
    Json.obj(
      "event" -> "alert".asJson,
      "level" -> evt.alertLevel.asJson,
      name(evt.metricName),
      ("message", Json.fromString(evt.message)),
      digest(evt.metricName),
      serviceId(evt),
      timestamp(evt)
    )

  private def actionStart(evt: ActionStart): Json =
    Json.obj(
      "event" -> "action_start".asJson,
      importance(evt.actionParams.importance),
      name(evt.metricId.metricName),
      measurement(evt.actionParams.metricId.metricName),
      traceInfo(evt),
      digest(evt.metricId.metricName),
      actionId(evt),
      serviceId(evt),
      timestamp(evt)
    )

  private def actionRetrying(evt: ActionRetry): Json =
    Json.obj(
      "event" -> "action_retry".asJson,
      importance(evt.actionParams.importance),
      name(evt.metricId.metricName),
      measurement(evt.actionParams.metricId.metricName),
      traceInfo(evt),
      ("cause", Json.fromString(evt.error.message)),
      digest(evt.metricId.metricName),
      actionId(evt),
      serviceId(evt),
      timestamp(evt)
    )

  private def actionFail(evt: ActionFail): Json =
    Json.obj(
      "event" -> "action_fail".asJson,
      importance(evt.actionParams.importance),
      name(evt.metricId.metricName),
      measurement(evt.actionParams.metricId.metricName),
      took(evt),
      traceInfo(evt),
      "notes" -> evt.output, // align with slack
      stackTrace(evt.error),
      digest(evt.metricId.metricName),
      actionId(evt),
      serviceId(evt),
      timestamp(evt)
    )

  private def actionComplete(evt: ActionComplete): Json =
    Json.obj(
      "event" -> "action_complete".asJson,
      importance(evt.actionParams.importance),
      name(evt.metricId.metricName),
      measurement(evt.actionParams.metricId.metricName),
      took(evt),
      traceInfo(evt),
      "result" -> evt.output, // align with slack
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
      .withInstantAlert(instantAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFail)
      .withActionComplete(actionComplete)

}
