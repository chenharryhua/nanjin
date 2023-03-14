package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.config.{Importance, MeasurementID}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import io.circe.syntax.*

private object SimpleJsonTranslator {
  import NJEvent.*

  private def timestamp(evt: NJEvent): (String, Json) = "timestamp" -> evt.timestamp.asJson
  private def serviceId(evt: NJEvent): (String, Json) = "serviceId" -> evt.serviceId.asJson
  private def serviceName(evt: NJEvent): (String, Json) =
    ("serviceName", Json.fromString(evt.serviceName.value))

  private def name(id: MeasurementID): (String, Json)     = "name" -> Json.fromString(id.name)
  private def digest(id: MeasurementID): (String, Json)   = "digest" -> Json.fromString(id.digest)
  private def actionId(evt: ActionEvent): (String, Json)  = "id" -> Json.fromString(evt.actionId)
  private def traceInfo(evt: ActionEvent): (String, Json) = "traceInfo" -> evt.actionInfo.traceInfo.asJson
  private def importance(imp: Importance): (String, Json) = "importance" -> imp.asJson

  private def stackTrace(err: NJError): (String, Json)    = "stackTrace" -> Json.fromString(err.stackTrace)
  private def metrics(ss: MetricSnapshot): (String, Json) = "metrics" -> ss.asJson
  private def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc           => "index" -> Json.Null
    case MetricIndex.Periodic(index) => "index" -> Json.fromInt(index)
  }

  private def policy(evt: ServiceEvent): (String, Json) = "policy" -> evt.serviceParams.restartPolicy.asJson

  private def serviceStarted(evt: ServiceStart): Json =
    Json.obj(
      "ServiceStart" ->
        Json.obj("params" -> evt.serviceParams.asJson, timestamp(evt)))

  private def servicePanic(evt: ServicePanic): Json =
    Json.obj(
      "ServicePanic" ->
        Json.obj(
          serviceName(evt),
          policy(evt),
          stackTrace(evt.error),
          serviceId(evt),
          timestamp(evt)
        ))

  private def serviceStopped(evt: ServiceStop): Json =
    Json.obj(
      "ServiceStop" ->
        Json.obj(
          serviceName(evt),
          ("exitCode", Json.fromInt(evt.cause.exitCode)),
          ("cause", Json.fromString(evt.cause.show)),
          policy(evt),
          serviceId(evt),
          timestamp(evt)
        ))

  private def metricReport(evt: MetricReport): Json =
    Json.obj(
      "MetricReport" ->
        Json.obj(
          metricIndex(evt.index),
          serviceName(evt),
          metrics(evt.snapshot),
          serviceId(evt),
          timestamp(evt)
        ))

  private def metricReset(evt: MetricReset): Json =
    Json.obj(
      "MetricReset" ->
        Json.obj(
          metricIndex(evt.index),
          serviceName(evt),
          metrics(evt.snapshot),
          serviceId(evt),
          timestamp(evt)
        ))

  private def passThrough(evt: PassThrough): Json =
    Json.obj(
      "PassThrough" ->
        Json.obj(
          name(evt.id),
          ("value", evt.value),
          digest(evt.id),
          serviceId(evt),
          timestamp(evt)
        ))

  private def instantAlert(evt: InstantAlert): Json =
    Json.obj(
      "Alert" ->
        Json.obj(
          "level" -> evt.alertLevel.asJson,
          name(evt.id),
          ("message", Json.fromString(evt.message)),
          digest(evt.id),
          serviceId(evt),
          timestamp(evt)))

  private def actionStart(evt: ActionStart): Json =
    Json.obj(
      "ActionStart" ->
        Json.obj(
          importance(evt.actionInfo.actionParams.importance),
          name(evt.id),
          traceInfo(evt),
          digest(evt.id),
          actionId(evt),
          serviceId(evt),
          timestamp(evt)
        ))

  private def actionRetrying(evt: ActionRetry): Json =
    Json.obj(
      "ActionRetry" ->
        Json.obj(
          importance(evt.actionInfo.actionParams.importance),
          name(evt.id),
          traceInfo(evt),
          ("cause", Json.fromString(evt.error.message)),
          digest(evt.id),
          actionId(evt),
          serviceId(evt),
          timestamp(evt)
        ))

  private def actionFail(evt: ActionFail): Json =
    Json.obj(
      "ActionFail" ->
        Json.obj(
          importance(evt.actionInfo.actionParams.importance),
          name(evt.id),
          traceInfo(evt),
          "notes" -> evt.output, // align with slack
          stackTrace(evt.error),
          digest(evt.id),
          actionId(evt),
          serviceId(evt),
          timestamp(evt)
        ))

  private def actionComplete(evt: ActionComplete): Json =
    Json.obj(
      "ActionComplete" ->
        Json.obj(
          importance(evt.actionInfo.actionParams.importance),
          name(evt.id),
          traceInfo(evt),
          "result" -> evt.output, // align with slack
          digest(evt.id),
          actionId(evt),
          serviceId(evt),
          timestamp(evt)
        ))

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(serviceStarted)
      .withServiceStop(serviceStopped)
      .withServicePanic(servicePanic)
      .withMetricReport(metricReport)
      .withMetricReset(metricReset)
      .withPassThrough(passThrough)
      .withInstantAlert(instantAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFail)
      .withActionComplete(actionComplete)
}
