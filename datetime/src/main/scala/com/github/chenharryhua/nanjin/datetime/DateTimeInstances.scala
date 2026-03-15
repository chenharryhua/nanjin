package com.github.chenharryhua.nanjin.datetime

import cats.{Hash, Order, Show}
import io.circe.{Decoder, Encoder}
import org.typelevel.cats.time.instances.all

import java.sql.{Date, Timestamp}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

/** `https://typelevel.org/cats-time/`
  */
trait DateTimeInstances extends all {

  given timestampInstance: Hash[Timestamp] & Order[Timestamp] & Show[Timestamp] =
    new Hash[Timestamp] with Order[Timestamp] with Show[Timestamp] {
      override def hash(x: Timestamp): Int = x.hashCode
      override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
      override def show(x: Timestamp): String = x.toString
    }

  given dateInstance: Hash[Date] & Order[Date] & Show[Date] =
    new Hash[Date] with Order[Date] with Show[Date] {
      override def hash(x: Date): Int = x.hashCode
      override def compare(x: Date, y: Date): Int = x.compareTo(y)
      override def show(x: Date): String = x.toString
    }

  given Encoder[Timestamp] =
    Encoder.encodeInstant.contramap[Timestamp](_.toInstant)
  given Decoder[Timestamp] =
    Decoder.decodeInstant.map[Timestamp](Timestamp.from)

  given Encoder[Date] =
    Encoder.encodeLocalDate.contramap[Date](_.toLocalDate)
  given Decoder[Date] =
    Decoder.decodeLocalDate.map[Date](Date.valueOf)

  given Encoder[FiniteDuration] =
    Encoder.encodeDuration.contramap[FiniteDuration](_.toJava)
  given Decoder[FiniteDuration] =
    Decoder.decodeDuration.map[FiniteDuration](_.toScala)
}
