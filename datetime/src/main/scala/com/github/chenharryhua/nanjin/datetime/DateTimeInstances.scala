package com.github.chenharryhua.nanjin.datetime

import cats.{Hash, Order, Show}
import cron4s.CronExpr
import io.chrisdavenport.cats.time.instances.all
import io.circe.{Decoder, Encoder}
import io.scalaland.enumz.Enum
import monocle.Iso

import java.sql.{Date, Timestamp}
import java.time.*
import java.util.concurrent.TimeUnit
import scala.compat.java8.DurationConverters.*
import scala.concurrent.duration.FiniteDuration

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

  implicit val encoderTimeUnit: Encoder[TimeUnit] = Encoder.encodeString.contramap(enumTimeUnit.getName)
  implicit val decoderTimeUnit: Decoder[TimeUnit] = Decoder.decodeString.map(enumTimeUnit.withName)
  implicit val showTimeUnit: Show[TimeUnit]       = enumTimeUnit.getName

  implicit val cronExprEncoder: Encoder[CronExpr]             = cron4s.circe.cronExprEncoder
  implicit val cronExprDecoder: Decoder[CronExpr]             = cron4s.circe.cronExprDecoder
  implicit val finiteDurationEncoder: Encoder[FiniteDuration] = Encoder[Duration].contramap(_.toJava)
  implicit val finiteDurationDecoder: Decoder[FiniteDuration] = Decoder[Duration].map(_.toScala)
}

private[datetime] trait Isos {

  implicit val isoInstant: Iso[Instant, Timestamp] =
    Iso[Instant, Timestamp](Timestamp.from)(_.toInstant)

  implicit val isoLocalDate: Iso[LocalDate, Date] =
    Iso[LocalDate, Date](a => Date.valueOf(a))(b => b.toLocalDate)

  implicit def isoLocalDateTime(implicit zoneId: ZoneId): Iso[LocalDateTime, Instant] =
    Iso[LocalDateTime, Instant](a => a.atZone(zoneId).toInstant)(b => LocalDateTime.ofInstant(b, zoneId))

  implicit def isoOffsetDatatime(implicit zoneId: ZoneId): Iso[OffsetDateTime, Instant] =
    Iso[OffsetDateTime, Instant](_.toInstant)(_.atZone(zoneId).toOffsetDateTime)

  implicit def isoZonedDatatime(implicit zoneId: ZoneId): Iso[ZonedDateTime, Instant] =
    Iso[ZonedDateTime, Instant](_.toInstant)(_.atZone(zoneId))

}
