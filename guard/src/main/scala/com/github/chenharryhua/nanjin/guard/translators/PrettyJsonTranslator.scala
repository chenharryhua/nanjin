package com.github.chenharryhua.nanjin.guard.translators
import cats.Applicative
import com.github.chenharryhua.nanjin.guard.config.MetricParams
import com.github.chenharryhua.nanjin.guard.event.MetricSnapshot
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import io.circe.Json

private object PrettyJsonTranslator {

  import jsonHelper.*

  private def prettyMetrics(ss: MetricSnapshot, mp: MetricParams): (String, Json) =
    "metrics" -> new SnapshotPolyglot(ss, mp).toPrettyJson

  // events handlers
  private def serviceStarted(evt: ServiceStart): Json =
    Json.obj(
      EventName.ServiceStart.camel ->
        Json.obj(serviceParams(evt.serviceParams), uptime(evt)))

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
          exitCode(evt.cause),
          exitCause(evt.cause)
        ))

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
          metricName(evt.metricName),
          metricDigest(evt.metricName),
          metricMeasurement(evt.metricName),
          serviceName(evt),
          serviceId(evt),
          alertMessage(evt)
        ))

  private def actionStart(evt: ActionStart): Json =
    Json.obj(
      EventName.ActionStart.camel ->
        Json.obj(
          actionId(evt),
          metricName(evt.actionParams.metricId.metricName),
          metricDigest(evt.actionParams.metricId.metricName),
          metricMeasurement(evt.actionParams.metricId.metricName),
          importance(evt),
          publishStrategy(evt),
          serviceName(evt),
          serviceId(evt),
          notes(evt.notes)
        ))

  private def actionRetrying(evt: ActionRetry): Json =
    Json.obj(
      EventName.ActionRetry.camel ->
        Json.obj(
          actionId(evt),
          metricName(evt.actionParams.metricId.metricName),
          metricDigest(evt.actionParams.metricId.metricName),
          metricMeasurement(evt.actionParams.metricId.metricName),
          importance(evt),
          publishStrategy(evt),
          serviceName(evt),
          serviceId(evt),
          policy(evt.actionParams),
          errCause(evt.error)
        ))

  private def actionFail(evt: ActionFail): Json =
    Json.obj(
      EventName.ActionFail.camel ->
        Json.obj(
          actionId(evt),
          metricName(evt.actionParams.metricId.metricName),
          metricDigest(evt.actionParams.metricId.metricName),
          metricMeasurement(evt.actionParams.metricId.metricName),
          importance(evt),
          publishStrategy(evt),
          serviceName(evt),
          serviceId(evt),
          took(evt),
          policy(evt.actionParams),
          notes(evt.notes),
          stackTrace(evt.error)
        ))

  private def actionDone(evt: ActionDone): Json =
    Json.obj(
      EventName.ActionDone.camel ->
        Json.obj(
          actionId(evt),
          metricName(evt.actionParams.metricId.metricName),
          metricDigest(evt.actionParams.metricId.metricName),
          metricMeasurement(evt.actionParams.metricId.metricName),
          importance(evt),
          publishStrategy(evt),
          serviceName(evt),
          serviceId(evt),
          took(evt),
          notes(evt.notes)
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
      .withActionDone(actionDone)

}
