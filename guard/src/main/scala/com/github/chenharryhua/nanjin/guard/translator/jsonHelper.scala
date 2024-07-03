package com.github.chenharryhua.nanjin.guard.translator

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionEvent, ServiceAlert}
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJError, NJEvent, ServiceStopCause}
import io.circe.Json
import io.circe.syntax.EncoderOps

object jsonHelper {

  def timestamp(evt: NJEvent): (String, Json)          = "timestamp" -> evt.timestamp.asJson
  def serviceId(evt: NJEvent): (String, Json)          = "serviceId" -> evt.serviceParams.serviceId.asJson
  def serviceParams(sp: ServiceParams): (String, Json) = "params" -> sp.asJson
  def exitCode(sc: ServiceStopCause): (String, Json)   = "exitCode" -> Json.fromInt(sc.exitCode)
  def exitCause(sc: ServiceStopCause): (String, Json)  = "exitCause" -> sc.asJson
  def actionId(evt: ActionEvent): (String, Json)       = "actionId" -> Json.fromInt(evt.actionID.uniqueToken)

  def policy(evt: NJEvent): (String, Json) =
    "policy" -> Json.fromString(evt.serviceParams.servicePolicies.restart.show)
  def policy(ap: ActionParams): (String, Json) = "policy" -> Json.fromString(ap.retryPolicy.show)
  def errorCause(err: NJError): (String, Json) = "cause" -> Json.fromString(err.message)
  def stack(err: NJError): (String, Json)      = "stack" -> err.stack.asJson

  def notes(js: Json): (String, Json) = "notes" -> js

  def metricName(mn: MetricName): (String, Json)        = "name" -> Json.fromString(mn.name)
  def metricDigest(mn: MetricName): (String, Json)      = "digest" -> Json.fromString(mn.digest)
  def metricMeasurement(id: MetricName): (String, Json) = "measurement" -> Json.fromString(id.measurement)

  def alertMessage(sa: ServiceAlert): (String, Json) = sa.alertLevel.entryName -> sa.message

  def serviceName(evt: NJEvent): (String, Json) =
    "serviceName" -> Json.fromString(evt.serviceParams.serviceName.value)

  def config(evt: ActionEvent): (String, Json) =
    "config" -> Json.fromString(evt.actionParams.configStr)

  def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc(_)       => "index" -> Json.Null
    case MetricIndex.Periodic(tick) => "index" -> Json.fromLong(tick.index)
  }
}
