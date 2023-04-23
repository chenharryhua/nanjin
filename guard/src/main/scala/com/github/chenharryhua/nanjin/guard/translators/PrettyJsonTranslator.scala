package com.github.chenharryhua.nanjin.guard.translators
import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, MetricName, MetricParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import io.circe.syntax.*

private object PrettyJsonTranslator {

  import NJEvent.*

  private def uptime(evt: NJEvent): (String, Json) =
    "upTime" -> Json.fromString(fmt.format(evt.upTime))
  private def serviceId(evt: NJEvent): (String, Json) =
    "serviceId" -> evt.serviceParams.serviceId.asJson
  private def actionName(metricName: MetricName): (String, Json) =
    "name" -> Json.fromString(metricName.display)
  private def actionId(evt: ActionEvent): (String, Json) =
    "id" -> Json.fromString(evt.actionId)
  private def traceId(evt: ActionEvent): (String, Json) =
    "traceId" -> evt.actionInfo.traceId.asJson
  private def took(evt: ActionResultEvent): (String, Json) =
    "took" -> Json.fromString(fmt.format(evt.took))
  private def stackTrace(err: NJError): (String, Json) =
    "stackTrace" -> Json.fromString(err.stackTrace)
  private def policy(evt: NJEvent): (String, Json) =
    "policy" -> Json.fromString(evt.serviceParams.restartPolicy)
  private def policy(ap: ActionParams): (String, Json) =
    "policy" -> Json.fromString(ap.retryPolicy)
  private def serviceName(evt: NJEvent): (String, Json) =
    "serviceName" -> Json.fromString(evt.serviceParams.serviceName)
  private def measurement(id: MetricName): (String, Json) =
    "measurement" -> Json.fromString(id.measurement)
  private def isCritical(evt: ActionEvent): (String, Json) =
    "importance" -> Json.fromString(evt.actionParams.importance.entryName)

  private def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc           => "index" -> Json.Null
    case MetricIndex.Periodic(index) => "index" -> Json.fromInt(index)
  }

  private def prettyMetrics(ss: MetricSnapshot, mp: MetricParams): (String, Json) =
    "metrics" -> new SnapshotJson(ss).toPrettyJson(mp)

  // events handlers
  private def serviceStarted(evt: ServiceStart): Json =
    Json.obj(
      EventName.ServiceStart.camel ->
        Json.obj("params" -> evt.serviceParams.asJson, uptime(evt)))

  private def servicePanic(evt: ServicePanic): Json =
    Json.obj(
      EventName.ServicePanic.camel ->
        Json.obj(serviceName(evt), serviceId(evt), uptime(evt), policy(evt), stackTrace(evt.error)))

  private def serviceStopped(evt: ServiceStop): Json =
    Json.obj(
      EventName.ServiceStop.camel ->
        Json.obj(
          serviceName(evt),
          serviceId(evt),
          uptime(evt),
          policy(evt),
          ("exitCode", Json.fromInt(evt.cause.exitCode)),
          ("cause", Json.fromString(evt.cause.show))))

  private def metricReport(evt: MetricReport): Json =
    Json.obj(
      EventName.MetricReport.camel ->
        Json.obj(
          metricIndex(evt.index),
          serviceName(evt),
          serviceId(evt),
          uptime(evt),
          prettyMetrics(evt.snapshot, evt.serviceParams.metricParams)))

  private def metricReset(evt: MetricReset): Json =
    Json.obj(
      EventName.MetricReset.camel ->
        Json.obj(
          metricIndex(evt.index),
          serviceName(evt),
          serviceId(evt),
          uptime(evt),
          prettyMetrics(evt.snapshot, evt.serviceParams.metricParams)))

  private def serviceAlert(evt: ServiceAlert): Json =
    Json.obj(
      EventName.ServiceAlert.camel ->
        Json.obj(
          actionName(evt.metricName),
          serviceName(evt),
          serviceId(evt),
          evt.alertLevel.entryName -> evt.message))

  private def actionStart(evt: ActionStart): Json =
    Json.obj(
      EventName.ActionStart.camel ->
        Json.obj(
          actionName(evt.metricId.metricName),
          isCritical(evt),
          serviceName(evt),
          serviceId(evt),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          traceId(evt),
          "notes" -> evt.notes.asJson
        ))

  private def actionRetrying(evt: ActionRetry): Json =
    Json.obj(
      EventName.ActionRetry.camel ->
        Json.obj(
          actionName(evt.metricId.metricName),
          isCritical(evt),
          serviceName(evt),
          serviceId(evt),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          traceId(evt),
          policy(evt.actionParams),
          ("cause", Json.fromString(evt.error.message))
        ))

  private def actionFail(evt: ActionFail): Json =
    Json.obj(
      EventName.ActionFail.camel ->
        Json.obj(
          actionName(evt.metricId.metricName),
          isCritical(evt),
          serviceName(evt),
          serviceId(evt),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          traceId(evt),
          took(evt),
          policy(evt.actionParams),
          "notes" -> evt.notes.asJson, // align with slack
          stackTrace(evt.error)
        ))

  private def actionComplete(evt: ActionComplete): Json =
    Json.obj(
      EventName.ActionComplete.camel ->
        Json.obj(
          actionName(evt.metricId.metricName),
          isCritical(evt),
          serviceName(evt),
          serviceId(evt),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          traceId(evt),
          took(evt),
          "notes" -> evt.notes.asJson // align with slack
        ))

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
