package com.github.chenharryhua.nanjin.guard.translator

import io.circe.optics.all.*
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import monocle.function.Plated

import java.time.Duration

object prettifyJson {

  private val pretty_json: Json => Json =
    Plated.transform[Json] { js =>
      js.asNumber match {
        case Some(value) => Json.fromString(decimalFormatter.format(value.toDouble))
        case None        =>
          js.as[Duration] match {
            case Left(_)      => js
            case Right(value) => Json.fromString(durationFormatter.format(value))
          }
      }
    }

  def apply[A: Encoder](a: A): Json = pretty_json(a.asJson)

}
