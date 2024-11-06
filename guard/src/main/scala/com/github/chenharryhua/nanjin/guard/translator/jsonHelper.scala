package com.github.chenharryhua.nanjin.guard.translator

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import com.github.chenharryhua.nanjin.guard.config.{MetricLabel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceMessage
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJError, NJEvent, ServiceStopCause}
import io.circe.Json
import io.circe.syntax.EncoderOps

object jsonHelper {

  def timestamp(evt: NJEvent): (String, Json)          = "timestamp" -> evt.timestamp.asJson
  def serviceId(sp: ServiceParams): (String, Json)     = "serviceId" -> sp.serviceId.asJson
  def serviceParams(sp: ServiceParams): (String, Json) = "params" -> sp.asJson
  def exitCode(sc: ServiceStopCause): (String, Json)   = "exitCode" -> Json.fromInt(sc.exitCode)
  def exitCause(sc: ServiceStopCause): (String, Json)  = "exitCause" -> sc.asJson

  def index(tick: Tick): (String, Json) = "index" -> Json.fromLong(tick.index)

  def policy(ap: Policy): (String, Json) = "policy" -> Json.fromString(ap.show)

  def stack(err: NJError): (String, Json) = "stack" -> err.stack.asJson

  def metricName(mn: MetricLabel): (String, Json) = "name" -> Json.fromString(mn.label)

  def jsonServiceMessage(sm: ServiceMessage): Json =
    sm.error
      .map(err => Json.obj(stack(err)))
      .asJson
      .deepMerge(
        Json.obj(
          serviceName(sm.serviceParams),
          serviceId(sm.serviceParams),
          sm.level.entryName -> sm.message
        ))

  def serviceName(sp: ServiceParams): (String, Json) =
    "serviceName" -> Json.fromString(sp.serviceName.value)

  def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc(_)       => "index" -> Json.Null
    case MetricIndex.Periodic(tick) => "index" -> Json.fromLong(tick.index)
  }
}
