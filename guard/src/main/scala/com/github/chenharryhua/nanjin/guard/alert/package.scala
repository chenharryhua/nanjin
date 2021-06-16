package com.github.chenharryhua.nanjin.guard

import cats.Show
import io.circe.{Decoder, Encoder}

import java.time.{Duration, LocalTime, ZoneId, ZonedDateTime}
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.FiniteDuration

package object alert {
  implicit private[alert] val finiteDurationEncoder: Encoder[FiniteDuration] = Encoder[Duration].contramap(_.toJava)
  implicit private[alert] val finiteDurationDecoder: Decoder[FiniteDuration] = Decoder[Duration].map(_.toScala)

  implicit private[alert] val showZonedDateTime: Show[ZonedDateTime] = _.toString
  implicit private[alert] val showZoneId: Show[ZoneId]               = _.toString
  implicit private[alert] val showLocalTime: Show[LocalTime]         = _.toString

  def toOrdinalWords(n: Int): String = n + {
    if (n % 100 / 10 == 1) "th" else (("thstndrd" + "th" * 6).sliding(2, 2).toSeq(n % 10))
  }

}
