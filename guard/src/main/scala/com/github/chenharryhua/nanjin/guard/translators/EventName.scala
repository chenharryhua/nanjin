package com.github.chenharryhua.nanjin.guard.translators

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
