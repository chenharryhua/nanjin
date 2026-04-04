package com.github.chenharryhua.nanjin.datetime

import cats.implicits.catsSyntaxEq
import cats.{Hash, Order, Show}
import io.circe.{Decoder, Encoder}
import org.typelevel.cats.time.instances.all

import java.sql.{Date, Timestamp}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

/** `https://typelevel.org/cats-time/`
  */
trait DateTimeInstances extends all {

  // Timestamp
  given Hash[Timestamp] with
    def hash(x: Timestamp): Int = x.hashCode
    def eqv(x: Timestamp, y: Timestamp): Boolean = x.compareTo(y) === 0

  given Order[Timestamp] with
    def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)

  given Show[Timestamp] with
    def show(x: Timestamp): String = x.toString

  // Date
  given Hash[Date] with
    def hash(x: Date): Int = x.hashCode
    def eqv(x: Date, y: Date): Boolean = x.compareTo(y) === 0

  given Order[Date] with
    def compare(x: Date, y: Date): Int = x.compareTo(y)

  given Show[Date] with
    def show(x: Date): String = x.toString

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
