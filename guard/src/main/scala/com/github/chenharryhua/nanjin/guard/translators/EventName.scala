package com.github.chenharryhua.nanjin.guard.translators

import enumeratum.{CatsEnum, Enum, EnumEntry}
import io.circe.Json
import io.circe.syntax.EncoderOps

sealed abstract class EventName(override val entryName: String) extends EnumEntry {
  val compact: String = entryName.replace(" ", "")
  val camel: String   = compact.take(1).toLowerCase + compact.tail
  val camelJson: Json = camel.asJson
  val snake: String   = entryName.replace(" ", "_").toLowerCase()
  val snakeJson: Json = snake.asJson
}

object EventName extends CatsEnum[EventName] with Enum[EventName] {
  override val values: IndexedSeq[EventName] = findValues

  object ServiceStart extends EventName("Service Start")
  object ServicePanic extends EventName("Service Panic")
  object ServiceStop extends EventName("Service Stop")
  object ServiceAlert extends EventName("Alert")
  object MetricReport extends EventName("Metric Report")
  object MetricReset extends EventName("Metric Reset")
  object ActionStart extends EventName("Action Start")
  object ActionRetry extends EventName("Action Retry")
  object ActionFail extends EventName("Action Fail")
  object ActionComplete extends EventName("Action Complete")
}
