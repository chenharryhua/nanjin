package com.github.chenharryhua.nanjin.guard

import io.circe.{Decoder, Encoder}

import java.time.Duration
import scala.compat.java8.DurationConverters.*
import scala.concurrent.duration.FiniteDuration

package object alert {
  implicit private[alert] val finiteDurationEncoder: Encoder[FiniteDuration] = Encoder[Duration].contramap(_.toJava)
  implicit private[alert] val finiteDurationDecoder: Decoder[FiniteDuration] = Decoder[Duration].map(_.toScala)

  def toOrdinalWords(n: Int): String = n + {
    if (n % 100 / 10 == 1) "th"
    else
      (n % 10) match {
        case 1 => "st"
        case 2 => "nd"
        case 3 => "rd"
        case _ => "th"
      }
  }
}
