package com.github.chenharryhua.nanjin.datetime

import cats.{Hash, Order, Show}
import io.circe.{Decoder, Encoder}
import io.scalaland.enumz.Enum
import monocle.Iso
import org.typelevel.cats.time.instances.all

import java.sql.{Date, Timestamp}
import java.time.*
import java.util.concurrent.TimeUnit
import scala.compat.java8.DurationConverters.*

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

  private[this] val enumTimeUnit: Enum[TimeUnit] = Enum[TimeUnit]

  implicit final val encoderTimeUnit: Encoder[TimeUnit] = Encoder.encodeString.contramap(enumTimeUnit.getName)
  implicit final val decoderTimeUnit: Decoder[TimeUnit] = Decoder.decodeString.map(enumTimeUnit.withName)
  implicit final val showTimeUnit: Show[TimeUnit]       = enumTimeUnit.getName

//  implicit final val cronExprEncoder: Encoder[CronExpr]             = cron4s.circe.cronExprEncoder
//  implicit final val cronExprDecoder: Decoder[CronExpr]             = cron4s.circe.cronExprDecoder
//  implicit final val finiteDurationEncoder: Encoder[FiniteDuration] = Encoder[Duration].contramap(_.toJava)
//  implicit final val finiteDurationDecoder: Decoder[FiniteDuration] = Decoder[Duration].map(_.toScala)

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

  implicit final def isoLocalDateTime(implicit zoneId: ZoneId): Iso[LocalDateTime, Instant] =
    Iso[LocalDateTime, Instant](a => a.atZone(zoneId).toInstant)(b => LocalDateTime.ofInstant(b, zoneId))

  implicit final def isoOffsetDatatime(implicit zoneId: ZoneId): Iso[OffsetDateTime, Instant] =
    Iso[OffsetDateTime, Instant](_.toInstant)(_.atZone(zoneId).toOffsetDateTime)

  implicit final def isoZonedDatatime(implicit zoneId: ZoneId): Iso[ZonedDateTime, Instant] =
    Iso[ZonedDateTime, Instant](_.toInstant)(_.atZone(zoneId))

}
