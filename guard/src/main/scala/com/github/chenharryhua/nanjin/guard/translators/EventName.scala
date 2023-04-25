package com.github.chenharryhua.nanjin.guard.translators

import com.github.chenharryhua.nanjin.guard.config.{ActionParams, MetricName}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJError, NJEvent}
import enumeratum.{CatsEnum, Enum, EnumEntry}
import io.circe.Json
import io.circe.syntax.EncoderOps

sealed abstract class EventName(override val entryName: String) extends EnumEntry with Product {
  val compact: String = entryName.replace(" ", "")
  val camel: String   = compact.take(1).toLowerCase + compact.tail
  val camelJson: Json = camel.asJson
  val snake: String   = entryName.replace(" ", "_").toLowerCase()
  val snakeJson: Json = snake.asJson
}

object EventName extends CatsEnum[EventName] with Enum[EventName] {
  override val values: IndexedSeq[EventName] = findValues

  case object ServiceStart extends EventName("Service Start")
  case object ServicePanic extends EventName("Service Panic")
  case object ServiceStop extends EventName("Service Stop")
  case object ServiceAlert extends EventName("Alert")
  case object MetricReport extends EventName("Metric Report")
  case object MetricReset extends EventName("Metric Reset")
  case object ActionStart extends EventName("Action Start")
  case object ActionRetry extends EventName("Action Retry")
  case object ActionFail extends EventName("Action Fail")
  case object ActionComplete extends EventName("Action Complete")
}

private object jsonInterpreter {

  def timestamp(evt: NJEvent): (String, Json)      = "timestamp" -> evt.timestamp.asJson
  def uptime(evt: NJEvent): (String, Json)         = "upTime" -> Json.fromString(fmt.format(evt.upTime))
  def serviceId(evt: NJEvent): (String, Json)      = "serviceId" -> evt.serviceParams.serviceId.asJson
  def actionId(evt: ActionEvent): (String, Json)   = "id" -> Json.fromString(evt.actionId)
  def traceId(evt: ActionEvent): (String, Json)    = "traceId" -> evt.actionInfo.traceId.asJson
  def took(evt: ActionResultEvent): (String, Json) = "took" -> Json.fromString(fmt.format(evt.took))
  def stackTrace(err: NJError): (String, Json)     = "stackTrace" -> Json.fromString(err.stackTrace)
  def policy(evt: NJEvent): (String, Json)     = "policy" -> Json.fromString(evt.serviceParams.restartPolicy)
  def policy(ap: ActionParams): (String, Json) = "policy" -> Json.fromString(ap.retryPolicy)
  def measurement(id: MetricName): (String, Json) = "measurement" -> Json.fromString(id.measurement)

  def serviceName(evt: NJEvent): (String, Json) =
    "serviceName" -> Json.fromString(evt.serviceParams.serviceName)

  def importance(evt: ActionEvent): (String, Json) =
    "importance" -> Json.fromString(evt.actionParams.importance.entryName)

  def publishStrategy(evt: ActionEvent): (String, Json) =
    "strategy" -> Json.fromString(evt.actionParams.publishStrategy.entryName)

  def metricIndex(index: MetricIndex): (String, Json) = index match {
    case MetricIndex.Adhoc           => "index" -> Json.Null
    case MetricIndex.Periodic(index) => "index" -> Json.fromInt(index)
  }
}

 object textConstant {
  @inline final val CONSTANT_ACTION_ID: String  = "ActionID"
  @inline final val CONSTANT_TRACE_ID: String   = "TraceID"
  @inline final val CONSTANT_TIMESTAMP: String  = "Timestamp"
  @inline final val CONSTANT_POLICY: String     = "Policy"
  @inline final val CONSTANT_CAUSE: String      = "Cause"
  @inline final val CONSTANT_TOOK: String       = "Took"
  @inline final val CONSTANT_DELAYED: String    = "Delayed"
  @inline final val CONSTANT_UPTIME: String     = "UpTime"
  @inline final val CONSTANT_BRIEF: String      = "Brief"
  @inline final val CONSTANT_METRICS: String    = "Metrics"
  @inline final val CONSTANT_TIMEZONE: String   = "TimeZone"
  @inline final val CONSTANT_SERVICE: String    = "Service"
  @inline final val CONSTANT_SERVICE_ID: String = "ServiceID"
  @inline final val CONSTANT_HOST: String       = "Host"
  @inline final val CONSTANT_TASK: String       = "Task"
  @inline final val CONSTANT_IMPORTANCE: String = "Importance"
  @inline final val CONSTANT_STRATEGY: String   = "Strategy"
}
