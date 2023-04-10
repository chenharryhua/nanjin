package com.github.chenharryhua.nanjin.guard.translators
import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, Importance, MetricName, MetricParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import io.circe.syntax.*

private object PrettyJsonTranslator {

  import NJEvent.*

  private def uptime(evt: NJEvent): (String, Json)    = "up_time" -> Json.fromString(fmt.format(evt.upTime))
  private def serviceId(evt: NJEvent): (String, Json) = "service_id" -> evt.serviceId.asJson
  private def name(metricName: MetricName): (String, Json) = "name" -> Json.fromString(metricName.show)
  private def actionId(evt: ActionEvent): (String, Json)   = "id" -> Json.fromString(evt.actionId)
  private def traceInfo(evt: ActionEvent): (String, Json)  = "trace_info" -> evt.actionInfo.traceInfo.asJson
  private def importance(imp: Importance): (String, Json)  = "importance" -> imp.asJson
  private def took(evt: ActionResultEvent): (String, Json) = "took" -> Json.fromString(fmt.format(evt.took))
  private def stackTrace(err: NJError): (String, Json)     = "stack_trace" -> Json.fromString(err.stackTrace)
  private def policy(evt: ServiceEvent): (String, Json)   = "policy" -> evt.serviceParams.restartPolicy.asJson
  private def policy(ap: ActionParams): (String, Json)    = "policy" -> ap.retryPolicy.asJson
  private def serviceName(evt: NJEvent): (String, Json)   = "service_name" -> evt.serviceName.value.asJson
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
      "service_start" ->
        Json.obj("params" -> evt.serviceParams.asJson, uptime(evt)))

  private def servicePanic(evt: ServicePanic): Json =
    Json.obj(
      "service_panic" ->
        Json.obj(serviceName(evt), serviceId(evt), uptime(evt), policy(evt), stackTrace(evt.error)))

  private def serviceStopped(evt: ServiceStop): Json =
    Json.obj(
      "service_stop" ->
        Json.obj(
          serviceName(evt),
          serviceId(evt),
          uptime(evt),
          policy(evt),
          ("exit_code", Json.fromInt(evt.cause.exitCode)),
          ("cause", Json.fromString(evt.cause.show))))

  private def metricReport(evt: MetricReport): Json =
    Json.obj(
      "metric_report" ->
        Json.obj(
          metricIndex(evt.index),
          serviceName(evt),
          serviceId(evt),
          uptime(evt),
          prettyMetrics(evt.snapshot, evt.serviceParams.metricParams)))

  private def metricReset(evt: MetricReset): Json =
    Json.obj(
      "metric_reset" ->
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
          name(evt.metricName),
          serviceName(evt),
          serviceId(evt),
          evt.alertLevel.show -> Json.fromString(evt.message)))

  private def actionStart(evt: ActionStart): Json =
    Json.obj(
      "action_start" ->
        Json.obj(
          name(evt.metricId.metricName),
          serviceName(evt),
          serviceId(evt),
          importance(evt.actionParams.importance),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          traceInfo(evt)
        ))

  private def actionRetrying(evt: ActionRetry): Json =
    Json.obj(
      "action_retry" ->
        Json.obj(
          name(evt.metricId.metricName),
          serviceName(evt),
          serviceId(evt),
          importance(evt.actionParams.importance),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          policy(evt.actionParams),
          traceInfo(evt),
          ("cause", Json.fromString(evt.error.message))
        ))

  private def actionFail(evt: ActionFail): Json =
    Json.obj(
      "action_fail" ->
        Json.obj(
          name(evt.metricId.metricName),
          serviceName(evt),
          serviceId(evt),
          importance(evt.actionParams.importance),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          took(evt),
          policy(evt.actionParams),
          traceInfo(evt),
          "notes" -> evt.output, // align with slack
          stackTrace(evt.error)
        ))

  private def actionComplete(evt: ActionComplete): Json =
    Json.obj(
      "action_complete" ->
        Json.obj(
          name(evt.metricId.metricName),
          serviceName(evt),
          serviceId(evt),
          importance(evt.actionParams.importance),
          measurement(evt.actionParams.metricId.metricName),
          actionId(evt),
          took(evt),
          traceInfo(evt),
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
