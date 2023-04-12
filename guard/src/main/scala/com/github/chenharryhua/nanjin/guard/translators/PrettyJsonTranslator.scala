package com.github.chenharryhua.nanjin.guard.translators
import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Importance, MetricName, MetricParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import io.circe.syntax.*

private object PrettyJsonTranslator {

  import NJEvent.*

  private def uptime(evt: NJEvent): (String, Json)    = "upTime" -> Json.fromString(fmt.format(evt.upTime))
  private def serviceId(evt: NJEvent): (String, Json) = "serviceId" -> evt.serviceId.asJson
  private def actionName(metricName: MetricName): (String, Json) = "name" -> metricName.display.asJson
  private def actionId(evt: ActionEvent): (String, Json)         = "id" -> Json.fromString(evt.actionId)
  private def traceId(evt: ActionEvent): (String, Json)          = "traceId" -> evt.actionInfo.traceId.asJson
  private def importance(imp: Importance): (String, Json)        = "importance" -> imp.asJson
  private def took(evt: ActionResultEvent): (String, Json) = "took" -> Json.fromString(fmt.format(evt.took))
  private def stackTrace(err: NJError): (String, Json)     = "stackTrace" -> Json.fromString(err.stackTrace)
  private def policy(evt: NJEvent): (String, Json)        = "policy" -> evt.serviceParams.restartPolicy.asJson
  private def policy(ap: ActionParams): (String, Json)    = "policy" -> ap.retryPolicy.asJson
  private def serviceName(evt: NJEvent): (String, Json)   = "serviceName" -> evt.serviceName.value.asJson
  private def measurement(id: MetricName): (String, Json) = "measurement" -> id.measurement.value.asJson

  private def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc           => "index" -> Json.Null
    case MetricIndex.Periodic(index) => "index" -> Json.fromInt(index)
  }

  private def prettyMetrics(ss: MetricSnapshot, mp: MetricParams): (String, Json) =
    "metrics" -> new SnapshotJson(ss).toPrettyJson(mp)

  // events handlers
  private def serviceStarted(evt: ServiceStart): Json =
    Json.obj(
      "serviceStart" ->
        Json.obj("params" -> evt.serviceParams.asJson, uptime(evt)))

  private def servicePanic(evt: ServicePanic): Json =
    Json.obj(
      "servicePanic" ->
        Json.obj(serviceName(evt), serviceId(evt), uptime(evt), policy(evt), stackTrace(evt.error)))

  private def serviceStopped(evt: ServiceStop): Json =
    Json.obj(
      "serviceStop" ->
        Json.obj(
          serviceName(evt),
          serviceId(evt),
          uptime(evt),
          policy(evt),
          ("exitCode", Json.fromInt(evt.cause.exitCode)),
          ("cause", Json.fromString(evt.cause.show))))

  private def metricReport(evt: MetricReport): Json =
    Json.obj(
      "metricReport" ->
        Json.obj(
          metricIndex(evt.index),
          serviceName(evt),
          serviceId(evt),
          uptime(evt),
          prettyMetrics(evt.snapshot, evt.serviceParams.metricParams)))

  private def metricReset(evt: MetricReset): Json =
    Json.obj(
      "metricReset" ->
        Json.obj(
          metricIndex(evt.index),
          serviceName(evt),
          serviceId(evt),
          uptime(evt),
          prettyMetrics(evt.snapshot, evt.serviceParams.metricParams)))

  private def instantAlert(evt: InstantAlert): Json =
    Json.obj(
      "alert" ->
        Json.obj(
          actionName(evt.metricName),
          serviceName(evt),
          serviceId(evt),
          evt.alertLevel.show -> Json.fromString(evt.message)))

  private def actionStart(evt: ActionStart): Json =
    Json.obj(
      "actionStart" ->
        Json.obj(
          actionName(evt.metricId.metricName),
          serviceName(evt),
          serviceId(evt),
          importance(evt.actionParams.importance),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          traceId(evt)
        ))

  private def actionRetrying(evt: ActionRetry): Json =
    Json.obj(
      "actionRetry" ->
        Json.obj(
          actionName(evt.metricId.metricName),
          serviceName(evt),
          serviceId(evt),
          importance(evt.actionParams.importance),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          traceId(evt),
          policy(evt.actionParams),
          ("cause", Json.fromString(evt.error.message))
        ))

  private def actionFail(evt: ActionFail): Json =
    Json.obj(
      "actionFail" ->
        Json.obj(
          actionName(evt.metricId.metricName),
          serviceName(evt),
          serviceId(evt),
          importance(evt.actionParams.importance),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          traceId(evt),
          took(evt),
          policy(evt.actionParams),
          "notes" -> evt.output, // align with slack
          stackTrace(evt.error)
        ))

  private def actionComplete(evt: ActionComplete): Json =
    Json.obj(
      "actionComplete" ->
        Json.obj(
          actionName(evt.metricId.metricName),
          serviceName(evt),
          serviceId(evt),
          importance(evt.actionParams.importance),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          traceId(evt),
          took(evt),
          "result" -> evt.output // align with slack
        ))

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
