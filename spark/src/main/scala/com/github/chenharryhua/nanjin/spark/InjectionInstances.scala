package com.github.chenharryhua.nanjin.spark

import java.sql.{Date, Timestamp}
import java.time._

import frameless.{Injection, SQLDate, SQLTimestamp}
import io.scalaland.chimney.Transformer
import monocle.Iso
import org.apache.spark.sql.catalyst.util.DateTimeUtils

private[spark] trait InjectionInstances extends Serializable {

  // monocle iso
  implicit val isoInstant: Iso[Instant, Timestamp] =
    Iso[Instant, Timestamp](Timestamp.from)(_.toInstant)

  implicit val isoLocalDate: Iso[LocalDate, JavaLocalDate] =
    Iso[LocalDate, JavaLocalDate](JavaLocalDate(_))(_.localDate)

  implicit val isoLocalTime: Iso[LocalTime, JavaLocalTime] =
    Iso[LocalTime, JavaLocalTime](JavaLocalTime(_))(_.localTime)

  implicit def isoLocalDateTime: Iso[LocalDateTime, JavaLocalDateTime] =
    Iso[LocalDateTime, JavaLocalDateTime](JavaLocalDateTime(_))(_.localDateTime)

  implicit val isoOffsetDateTime: Iso[OffsetDateTime, JavaOffsetDateTime] =
    Iso[OffsetDateTime, JavaOffsetDateTime](JavaOffsetDateTime(_))(_.offsetDateTime)

  implicit val isoZonedDateTime: Iso[ZonedDateTime, JavaZonedDateTime] =
    Iso[ZonedDateTime, JavaZonedDateTime](JavaZonedDateTime(_))(_.zonedDateTime)

  // frameless injection
  implicit val javaSQLTimestampInjection: Injection[Timestamp, SQLTimestamp] =
    Injection[Timestamp, SQLTimestamp](
      a => SQLTimestamp(DateTimeUtils.fromJavaTimestamp(a)),
      b => DateTimeUtils.toJavaTimestamp(b.us))

  implicit val javaSQLDateInjection: Injection[Date, SQLDate] =
    Injection[Date, SQLDate](
      a => SQLDate(DateTimeUtils.fromJavaDate(a)),
      b => DateTimeUtils.toJavaDate(b.days))

  implicit def isoInjection[A, B](implicit iso: Iso[A, B]): Injection[A, B] =
    Injection[A, B](iso.get, iso.reverseGet)

  // chimney transformers
  implicit def chimneyTransform[A, B](implicit iso: Iso[A, B]): Transformer[A, B] =
    (src: A) => iso.get(src)

  implicit def chimneyTransform2[A, B](implicit iso: Iso[A, B]): Transformer[B, A] =
    (src: B) => iso.reverseGet(src)

  implicit val instantTransform: Transformer[Instant, Timestamp] =
    (src: Instant) => new Timestamp(src.toEpochMilli)

  implicit val instantTransform2: Transformer[Timestamp, Instant] =
    (src: Timestamp) => src.toInstant

  implicit val localDateTransform: Transformer[LocalDate, Date] =
    (src: LocalDate) => Date.valueOf(src)

  implicit val localDateTransform2: Transformer[Date, LocalDate] =
    (src: Date) => src.toLocalDate

  // can not go back
  implicit val zonedDateTimeTransform: Transformer[ZonedDateTime, Timestamp] =
    (src: ZonedDateTime) => Timestamp.from(src.toInstant)

  implicit val offsetDateTimeTransform: Transformer[OffsetDateTime, Timestamp] =
    (src: OffsetDateTime) => Timestamp.from(src.toInstant)
}
