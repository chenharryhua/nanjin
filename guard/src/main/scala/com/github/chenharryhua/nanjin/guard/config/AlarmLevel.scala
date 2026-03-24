package com.github.chenharryhua.nanjin.guard.config

import cats.{Order, Show}
import cats.implicits.catsSyntaxEq
import io.circe.{Decoder, Encoder}
import org.slf4j.event.Level

enum AlarmLevel(val value: Int, val level: Level):
  case Error extends AlarmLevel(4, Level.ERROR)
  case Warn extends AlarmLevel(3, Level.WARN)
  case Good extends AlarmLevel(2, Level.INFO)
  case Info extends AlarmLevel(1, Level.INFO)
  case Debug extends AlarmLevel(0, Level.DEBUG)

object AlarmLevel:
  given Encoder[AlarmLevel] = Encoder.encodeString.contramap(_.productPrefix)
  given Decoder[AlarmLevel] = Decoder.decodeString.emap(s =>
    AlarmLevel.values.find(_.productPrefix === s).toRight(s"Invalid AlarmLevel: $s"))
  given Show[AlarmLevel] = _.productPrefix
  given Order[AlarmLevel] = Order.by(_.value)
