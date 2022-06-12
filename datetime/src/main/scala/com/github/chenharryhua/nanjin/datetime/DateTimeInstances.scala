package com.github.chenharryhua.nanjin.datetime

import cats.{Hash, Order, Show}
import io.circe.{Decoder, Encoder}
import monocle.Iso
import org.typelevel.cats.time.instances.all

import java.sql.{Date, Timestamp}
import java.time.*

/** [[https://typelevel.org/cats-time/]]
  */
private[datetime] trait DateTimeInstances extends all {

  implicit final val timestampInstance: Hash[Timestamp] & Order[Timestamp] & Show[Timestamp] =
    new Hash[Timestamp] with Order[Timestamp] with Show[Timestamp] {
      override def hash(x: Timestamp): Int                  = x.hashCode
      override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
      override def show(x: Timestamp): String               = x.toString
    }

  implicit final val dateInstance: Hash[Date] & Order[Date] & Show[Date] =
    new Hash[Date] with Order[Date] with Show[Date] {
      override def hash(x: Date): Int             = x.hashCode
      override def compare(x: Date, y: Date): Int = x.compareTo(y)
      override def show(x: Date): String          = x.toString
    }

  implicit final val timestampCirceEncoder: Encoder[Timestamp] =
    Encoder.encodeInstant.contramap[Timestamp](_.toInstant)
  implicit final val timestampCirceDecoder: Decoder[Timestamp] =
    Decoder.decodeInstant.map[Timestamp](Timestamp.from)

  implicit final val dateCirceEncoder: Encoder[Date] = Encoder.encodeLocalDate.contramap[Date](_.toLocalDate)
  implicit final val dateCirceDecoder: Decoder[Date] = Decoder.decodeLocalDate.map[Date](Date.valueOf)
}

private[datetime] trait Isos {

  implicit final val isoInstant: Iso[Instant, Timestamp] =
    Iso[Instant, Timestamp](Timestamp.from)(_.toInstant)

  implicit final val isoLocalDate: Iso[LocalDate, Date] =
    Iso[LocalDate, Date](a => Date.valueOf(a))(b => b.toLocalDate)

}
