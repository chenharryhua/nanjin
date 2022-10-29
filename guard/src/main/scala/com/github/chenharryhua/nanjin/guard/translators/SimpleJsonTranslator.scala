package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.config.Digested
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import io.circe.syntax.*

private object SimpleJsonTranslator {
  import NJEvent.*

  private def timestamp(evt: NJEvent): (String, Json) = "timestamp" -> evt.timestamp.asJson
  private def serviceId(evt: NJEvent): (String, Json) = "serviceId" -> evt.serviceId.asJson
  private def serviceName(evt: NJEvent): (String, Json) =
    ("serviceName", Json.fromString(evt.serviceName.value))

  private def name(dg: Digested): (String, Json)          = "name" -> Json.fromString(dg.name)
  private def digest(dg: Digested): (String, Json)        = "digest" -> Json.fromString(dg.digest)
  private def actionId(evt: ActionEvent): (String, Json)  = "id" -> Json.fromInt(evt.actionId)
  private def traceInfo(evt: ActionEvent): (String, Json) = "traceInfo" -> evt.actionInfo.traceInfo.asJson

  private def stackTrace(err: NJError): (String, Json)    = "stackTrace" -> Json.fromString(err.stackTrace)
  private def metrics(ss: MetricSnapshot): (String, Json) = "metrics" -> ss.asJson
  private def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc           => "index" -> Json.Null
    case MetricIndex.Periodic(index) => "index" -> Json.fromInt(index)
  }

  private def policy(evt: ServiceEvent): (String, Json) = "policy" -> evt.serviceParams.retryPolicy.asJson

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
          name(evt.digested),
          ("isError", Json.fromBoolean(evt.isError)),
          ("value", evt.value),
          digest(evt.digested),
          serviceId(evt),
          timestamp(evt)
        ))

  private def instantAlert(evt: InstantAlert): Json =
    Json.obj(
      "Alert" ->
        Json.obj(
          name(evt.digested),
          ("importance", evt.importance.asJson),
          ("message", Json.fromString(evt.message)),
          digest(evt.digested),
          serviceId(evt),
          timestamp(evt)
        ))

  private def actionStart(evt: ActionStart): Json =
    Json.obj(
      "ActionStart" ->
        Json.obj(
          name(evt.digested),
          traceInfo(evt),
          ("input", evt.input),
          digest(evt.digested),
          actionId(evt),
          serviceId(evt),
          timestamp(evt)
        ))

  private def actionRetrying(evt: ActionRetry): Json =
    Json.obj(
      "ActionRetry" ->
        Json.obj(
          name(evt.digested),
          traceInfo(evt),
          ("cause", Json.fromString(evt.error.message)),
          digest(evt.digested),
          actionId(evt),
          serviceId(evt),
          timestamp(evt)
        ))

  private def actionFailed(evt: ActionFail): Json =
    Json.obj(
      "ActionFail" ->
        Json.obj(
          name(evt.digested),
          traceInfo(evt),
          ("input", evt.input),
          stackTrace(evt.error),
          digest(evt.digested),
          actionId(evt),
          serviceId(evt),
          timestamp(evt)
        ))

  private def actionSucced(evt: ActionSucc): Json =
    Json.obj(
      "ActionSucc" ->
        Json.obj(
          name(evt.digested),
          traceInfo(evt),
          ("output", evt.output),
          digest(evt.digested),
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
      .withActionFail(actionFailed)
      .withActionSucc(actionSucced)
}
