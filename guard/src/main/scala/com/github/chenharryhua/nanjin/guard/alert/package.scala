package com.github.chenharryhua.nanjin.guard

import cats.Show
import io.circe.{Decoder, Encoder}

import java.time.{Duration, Instant, ZoneId}
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.FiniteDuration

package object alert {
  implicit private[alert] val finiteDurationEncoder: Encoder[FiniteDuration] = Encoder[Duration].contramap(_.toJava)
  implicit private[alert] val finiteDurationDecoder: Decoder[FiniteDuration] = Decoder[Duration].map(_.toScala)

  implicit private[alert] val showInstant: Show[Instant] = _.toString
  implicit private[alert] val showZoneId: Show[ZoneId]   = _.toString

}
