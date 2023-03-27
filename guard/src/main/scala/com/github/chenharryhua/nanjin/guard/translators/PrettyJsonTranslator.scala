package com.github.chenharryhua.nanjin.guard.translators
import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Importance, MetricName, MetricParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import io.circe.syntax.*

private object PrettyJsonTranslator {

  import NJEvent.*

  private def uptime(evt: NJEvent): (String, Json) = "upTime" -> Json.fromString(fmt.format(evt.upTime))

  private def serviceId(evt: NJEvent): (String, Json) = "serviceId" -> evt.serviceId.asJson

  private def serviceName(evt: NJEvent): (String, Json) =
    ("serviceName", Json.fromString(evt.serviceName.value))

  private def name(metricName: MetricName): (String, Json) = "name" -> Json.fromString(metricName.show)

  private def measurement(id: MetricName): (String, Json) =
    "measurement" -> Json.fromString(id.measurement.value)

  private def actionId(evt: ActionEvent): (String, Json) = "id" -> Json.fromString(evt.actionId)

  private def traceInfo(evt: ActionEvent): (String, Json) = "traceInfo" -> evt.actionInfo.traceInfo.asJson

  private def importance(imp: Importance): (String, Json) = "importance" -> imp.asJson

  private def took(evt: ActionResultEvent): (String, Json) = "took" -> evt.took.asJson

  private def stackTrace(err: NJError): (String, Json) = "stackTrace" -> Json.fromString(err.stackTrace)

  private def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc           => "index" -> Json.Null
    case MetricIndex.Periodic(index) => "index" -> Json.fromInt(index)
  }

  private def policy(evt: ServiceEvent): (String, Json) = "policy" -> evt.serviceParams.restartPolicy.asJson
  private def policy(actionParams: ActionParams): (String, Json) = "policy" -> actionParams.retryPolicy.asJson

  private def prettyMetrics(ss: MetricSnapshot, mp: MetricParams): (String, Json) =
    "metrics" -> new SnapshotJson(ss).toPrettyJson(mp)

  // events handlers
  private def serviceStarted(evt: ServiceStart): Json =
    Json.obj(
      "ServiceStart" ->
        Json.obj("params" -> evt.serviceParams.asJson, uptime(evt)))

  private def servicePanic(evt: ServicePanic): Json =
    Json.obj(
      "ServicePanic" ->
        Json.obj(
          serviceName(evt),
          serviceId(evt),
          uptime(evt),
          policy(evt),
          stackTrace(evt.error)
        ))

  private def serviceStopped(evt: ServiceStop): Json =
    Json.obj(
      "ServiceStop" ->
        Json.obj(
          serviceName(evt),
          serviceId(evt),
          uptime(evt),
          policy(evt),
          ("exitCode", Json.fromInt(evt.cause.exitCode)),
          ("cause", Json.fromString(evt.cause.show))
        ))

  private def metricReport(evt: MetricReport): Json =
    Json.obj(
      "MetricReport" ->
        Json.obj(
          metricIndex(evt.index),
          uptime(evt),
          prettyMetrics(evt.snapshot, evt.serviceParams.metricParams),
          serviceName(evt),
          serviceId(evt)
        ))

  private def metricReset(evt: MetricReset): Json =
    Json.obj(
      "MetricReset" ->
        Json.obj(
          metricIndex(evt.index),
          uptime(evt),
          prettyMetrics(evt.snapshot, evt.serviceParams.metricParams),
          serviceName(evt),
          serviceId(evt)
        ))

  private def instantAlert(evt: InstantAlert): Json =
    Json.obj(
      "Alert" ->
        Json.obj(
          "level" -> evt.alertLevel.asJson,
          name(evt.metricName),
          ("message", Json.fromString(evt.message)),
          serviceName(evt),
          serviceId(evt)
        ))

  private def actionStart(evt: ActionStart): Json =
    Json.obj(
      "ActionStart" ->
        Json.obj(
          name(evt.metricID.metricName),
          importance(evt.actionInfo.actionParams.importance),
          measurement(evt.actionParams.metricID.metricName),
          actionId(evt),
          traceInfo(evt),
          serviceName(evt),
          serviceId(evt)
        ))

  private def actionRetrying(evt: ActionRetry): Json =
    Json.obj(
      "ActionRetry" ->
        Json.obj(
          name(evt.metricID.metricName),
          importance(evt.actionInfo.actionParams.importance),
          measurement(evt.actionParams.metricID.metricName),
          actionId(evt),
          policy(evt.actionParams),
          traceInfo(evt),
          ("cause", Json.fromString(evt.error.message)),
          serviceName(evt),
          serviceId(evt)
        ))

  private def actionFail(evt: ActionFail): Json =
    Json.obj(
      "ActionFail" ->
        Json.obj(
          name(evt.metricID.metricName),
          importance(evt.actionInfo.actionParams.importance),
          measurement(evt.actionParams.metricID.metricName),
          actionId(evt),
          took(evt),
          policy(evt.actionParams),
          traceInfo(evt),
          "notes" -> evt.output, // align with slack
          stackTrace(evt.error),
          serviceName(evt),
          serviceId(evt)
        ))

  private def actionComplete(evt: ActionComplete): Json =
    Json.obj(
      "ActionComplete" ->
        Json.obj(
          name(evt.metricID.metricName),
          importance(evt.actionInfo.actionParams.importance),
          measurement(evt.actionParams.metricID.metricName),
          actionId(evt),
          took(evt),
          traceInfo(evt),
          "result" -> evt.output, // align with slack
          serviceName(evt),
          serviceId(evt)
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
