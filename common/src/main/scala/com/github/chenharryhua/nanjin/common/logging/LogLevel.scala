package com.github.chenharryhua.nanjin.common.logging

import cats.syntax.eq.given
import cats.{Order, Show}
import io.circe.{Decoder, Encoder}

// name corresponding to org.slf4j.event.Level
enum LogLevel(val value: Int):
  case Error extends LogLevel(4)
  case Warn extends LogLevel(3)
  case Good extends LogLevel(2)
  case Info extends LogLevel(1)
  case Debug extends LogLevel(0)
end LogLevel

object LogLevel:
  given Encoder[LogLevel] = Encoder.encodeString.contramap(_.productPrefix)
  given Decoder[LogLevel] = Decoder.decodeString.emap(s =>
    LogLevel.values.find(_.productPrefix === s).toRight(s"Invalid LogLevel: $s"))
  given Show[LogLevel] = _.productPrefix
  given Order[LogLevel] = Order.by(_.value)
end LogLevel
