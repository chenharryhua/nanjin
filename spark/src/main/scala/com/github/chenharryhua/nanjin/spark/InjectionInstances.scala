package com.github.chenharryhua.nanjin.spark

import java.sql.{Date, Timestamp}
import java.time._

import doobie.util.Meta
import frameless.{Injection, SQLDate, SQLTimestamp}
import org.apache.spark.sql.catalyst.util.DateTimeUtils

private[spark] trait InjectionInstances extends Serializable {

  implicit object javaSQLTimestampInjection extends Injection[Timestamp, SQLTimestamp] {

    override def apply(a: Timestamp): SQLTimestamp =
      SQLTimestamp(DateTimeUtils.fromJavaTimestamp(a))
    override def invert(b: SQLTimestamp): Timestamp = DateTimeUtils.toJavaTimestamp(b.us)
  }

  implicit object instantInjection extends Injection[Instant, Timestamp] {

    override def apply(a: Instant): Timestamp  = Timestamp.from(a)
    override def invert(b: Timestamp): Instant = b.toInstant
  }

  implicit object javaSQLDateInjection extends Injection[Date, SQLDate] {
    override def apply(a: Date): SQLDate  = SQLDate(DateTimeUtils.fromJavaDate(a))
    override def invert(b: SQLDate): Date = DateTimeUtils.toJavaDate(b.days)
  }

  implicit object localDateInjection extends Injection[LocalDate, Date] {
    override def apply(a: LocalDate): Date  = Date.valueOf(a)
    override def invert(b: Date): LocalDate = b.toLocalDate
  }

  implicit def localDateTimeInjection(implicit zoneId: ZoneId): Injection[LocalDateTime, Instant] =
    new Injection[LocalDateTime, Instant] {
      override def apply(a: LocalDateTime): Instant  = a.atZone(zoneId).toInstant
      override def invert(b: Instant): LocalDateTime = LocalDateTime.ofInstant(b, zoneId)
    }

  implicit def zonedDateTimeInjection(implicit zoneId: ZoneId): Injection[ZonedDateTime, Instant] =
    new Injection[ZonedDateTime, Instant] {
      override def apply(a: ZonedDateTime): Instant  = a.toInstant
      override def invert(b: Instant): ZonedDateTime = ZonedDateTime.ofInstant(b, zoneId)
    }

//doobie
  implicit def inferDoobieMeta[A, B](implicit in: Injection[A, B], mb: Meta[B]): Meta[A] =
    Meta[B].imap(in.invert)(in.apply)
}
