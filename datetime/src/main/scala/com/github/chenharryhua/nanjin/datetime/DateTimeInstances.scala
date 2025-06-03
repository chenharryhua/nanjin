package com.github.chenharryhua.nanjin.datetime

import cats.{Hash, Order, Show}
import io.circe.{Decoder, Encoder}
import org.typelevel.cats.time.instances.all

import java.sql.{Date, Timestamp}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

/** [[https://typelevel.org/cats-time/]]
  */
trait DateTimeInstances extends all {

  implicit final val timestampInstance: Hash[Timestamp] & Order[Timestamp] & Show[Timestamp] =
    new Hash[Timestamp] with Order[Timestamp] with Show[Timestamp] {
      override def hash(x: Timestamp): Int = x.hashCode
      override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
      override def show(x: Timestamp): String = x.toString
    }

  implicit final val dateInstance: Hash[Date] & Order[Date] & Show[Date] =
    new Hash[Date] with Order[Date] with Show[Date] {
      override def hash(x: Date): Int = x.hashCode
      override def compare(x: Date, y: Date): Int = x.compareTo(y)
      override def show(x: Date): String = x.toString
    }

  implicit final val timestampCirceEncoder: Encoder[Timestamp] =
    Encoder.encodeInstant.contramap[Timestamp](_.toInstant)
  implicit final val timestampCirceDecoder: Decoder[Timestamp] =
    Decoder.decodeInstant.map[Timestamp](Timestamp.from)

  implicit final val dateCirceEncoder: Encoder[Date] =
    Encoder.encodeLocalDate.contramap[Date](_.toLocalDate)
  implicit final val dateCirceDecoder: Decoder[Date] =
    Decoder.decodeLocalDate.map[Date](Date.valueOf)

  implicit final val finiteDurationCirceEncoder: Encoder[FiniteDuration] =
    Encoder.encodeDuration.contramap[FiniteDuration](_.toJava)
  implicit final val finiteDurationCirceDecoder: Decoder[FiniteDuration] =
    Decoder.decodeDuration.map[FiniteDuration](_.toScala)
}
