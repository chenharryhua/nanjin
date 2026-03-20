package com.github.chenharryhua.nanjin.guard.config

import com.github.chenharryhua.nanjin.guard.config.LogFormat.findValues
import enumeratum.{CatsEnum, CirceEnum, Enum, EnumEntry}

sealed trait LogFormat extends EnumEntry
object LogFormat extends Enum[LogFormat] with CirceEnum[LogFormat] with CatsEnum[LogFormat] {
  override def values: IndexedSeq[LogFormat] = findValues

  case object Console_PlainText extends LogFormat
  case object Console_Json_OneLine extends LogFormat
  case object Console_Json_MultiLine extends LogFormat
  case object Console_JsonVerbose extends LogFormat

  case object Slf4j_PlainText extends LogFormat
  case object Slf4j_Json_OneLine extends LogFormat
  case object Slf4j_Json_MultiLine extends LogFormat
}
