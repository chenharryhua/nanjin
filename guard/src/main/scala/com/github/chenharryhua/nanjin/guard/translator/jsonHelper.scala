package com.github.chenharryhua.nanjin.guard.translator

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import com.github.chenharryhua.nanjin.guard.event.{Error, Event, MetricIndex, ServiceStopCause}
import io.circe.Json
import io.circe.syntax.EncoderOps

object jsonHelper {

  def timestamp(evt: Event): (String, Json) = "timestamp" -> evt.timestamp.asJson
  def service_id(sp: ServiceParams): (String, Json) = "service_id" -> sp.serviceId.asJson
  def exit_code(sc: ServiceStopCause): (String, Json) = "exit_code" -> Json.fromInt(sc.exitCode)
  def exit_cause(sc: ServiceStopCause): (String, Json) = "exit_cause" -> sc.asJson

  def index(tick: Tick): (String, Json) = "index" -> Json.fromLong(tick.index)

  def policy(ap: Policy): (String, Json) = "policy" -> Json.fromString(ap.show)

  def stack(err: Error): (String, Json) = "stack" -> err.stack.asJson

  def json_service_message(sm: ServiceMessage): Json =
    sm.error
      .map(err => Json.obj(stack(err)))
      .asJson
      .deepMerge(
        Json.obj(
          service_name(sm.serviceParams),
          service_id(sm.serviceParams),
          "token" -> sm.token.asJson,
          sm.level.entryName -> sm.message
        ))

  def service_name(sp: ServiceParams): (String, Json) =
    "service_name" -> Json.fromString(sp.serviceName.value)

  def metric_index(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc(_)       => "index" -> Json.Null
    case MetricIndex.Periodic(tick) => "index" -> Json.fromLong(tick.index)
  }
}
