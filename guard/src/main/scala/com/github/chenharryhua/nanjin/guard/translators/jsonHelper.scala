package com.github.chenharryhua.nanjin.guard.translators

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionEvent, ActionResultEvent, ServiceAlert}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJError, NJEvent, ServiceStopCause}
import io.circe.Json
import io.circe.syntax.EncoderOps

private object jsonHelper {

  def timestamp(evt: NJEvent): (String, Json)          = "timestamp" -> evt.timestamp.asJson
  def uptime(evt: NJEvent): (String, Json)             = "upTime" -> Json.fromString(fmt.format(evt.upTime))
  def serviceId(evt: NJEvent): (String, Json)          = "serviceId" -> evt.serviceParams.serviceId.asJson
  def serviceParams(sp: ServiceParams): (String, Json) = "params" -> sp.asJson
  def exitCode(sc: ServiceStopCause): (String, Json)   = "exitCode" -> Json.fromInt(sc.exitCode)
  def exitCause(sc: ServiceStopCause): (String, Json)  = "exitCause" -> Json.fromString(sc.show)

  def policy(evt: NJEvent): (String, Json) =
    "policy" -> Json.fromString(evt.serviceParams.servicePolicies.restart.show)
  def policy(ap: ActionParams): (String, Json) = "policy" -> Json.fromString(ap.retryPolicy.show)
  def errCause(err: NJError): (String, Json)   = "cause" -> Json.fromString(err.message)
  def stackTrace(err: NJError): (String, Json) = "stackTrace" -> Json.fromString(err.stackTrace)

  def actionId(evt: ActionEvent): (String, Json)   = "id" -> Json.fromInt(evt.actionId)
  def took(evt: ActionResultEvent): (String, Json) = "took" -> Json.fromString(fmt.format(evt.took))

  def notes(oj: Option[Json]): (String, Json) = "notes" -> oj.asJson

  def metricName(mn: MetricName): (String, Json)        = "name" -> Json.fromString(mn.value)
  def metricDigest(mn: MetricName): (String, Json)      = "digest" -> Json.fromString(mn.digest)
  def metricMeasurement(id: MetricName): (String, Json) = "measurement" -> Json.fromString(id.measurement)

  def alertMessage(sa: ServiceAlert): (String, Json) = sa.alertLevel.entryName -> sa.message

  def serviceName(evt: NJEvent): (String, Json) =
    "serviceName" -> Json.fromString(evt.serviceParams.serviceName)

  def config(evt: ActionEvent): (String, Json) =
    "config" -> Json.fromString(evt.actionParams.configStr)

  def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc          => "index" -> Json.Null
    case MetricIndex.Periodic(tick) => "index" -> Json.fromLong(tick.index)
  }
}
