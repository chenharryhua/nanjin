package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.syntax.all.catsSyntaxEq
import io.circe.{Decoder, Encoder}

enum LogFormat:
  case ConsolePlainText,
    ConsoleJson,
    ConsoleJsonMultiLine,
    ConsoleJsonVerbose,
    Slf4jJson
end LogFormat

object LogFormat:
  given Encoder[LogFormat] = Encoder.encodeString.contramap(_.productPrefix)
  given Decoder[LogFormat] = Decoder.decodeString.emap { s =>
    LogFormat.values.find(_.productPrefix === s).toRight(s"invalid LogFormat: $s")
  }
  given Show[LogFormat] = _.productPrefix
end LogFormat
