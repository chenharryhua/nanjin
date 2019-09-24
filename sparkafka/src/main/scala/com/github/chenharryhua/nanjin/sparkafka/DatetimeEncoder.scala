package com.github.chenharryhua.nanjin.sparkafka

import java.sql.{Date, Timestamp}
import java.time._

import doobie.util.Meta
import frameless.Injection

object DatetimeEncoder {
//typed-spark
  implicit object instantInjection extends Injection[Instant, String] {
    override def apply(a: Instant): String  = a.toString
    override def invert(b: String): Instant = Instant.parse(b)
  }

  implicit object localDateTimeInjection extends Injection[LocalDateTime, String] {
    override def apply(a: LocalDateTime): String  = a.toString
    override def invert(b: String): LocalDateTime = LocalDateTime.parse(b)
  }

  implicit object zonedDateTimeInjection extends Injection[ZonedDateTime, String] {
    override def apply(a: ZonedDateTime): String  = a.toString
    override def invert(b: String): ZonedDateTime = ZonedDateTime.parse(b)
  }

  implicit object localdateInjection extends Injection[LocalDate, String] {
    override def apply(a: LocalDate): String  = a.toString
    override def invert(b: String): LocalDate = LocalDate.parse(b)
  }

  implicit object sqlDateInjection extends Injection[Date, Long] {
    override def apply(a: Date): Long  = a.getTime()
    override def invert(b: Long): Date = new Date(b)
  }

  implicit object sqlTimestampInjection extends Injection[Timestamp, Long] {
    override def apply(a: Timestamp): Long  = a.getTime()
    override def invert(b: Long): Timestamp = new Timestamp(b)
  }

//doobie
  implicit val doobieInstantMeta: Meta[Instant] = Meta[Timestamp].timap(_.toInstant)(Timestamp.from)

  implicit val doobieLocalDateTimeMeta: Meta[LocalDateTime] =
    Meta[Timestamp].timap(_.toLocalDateTime)(Timestamp.valueOf)

  implicit val doobieZonedDateTimeMeta: Meta[ZonedDateTime] =
    Meta[Timestamp].timap(_.toLocalDateTime.atZone(ZoneId.systemDefault()))(x =>
      Timestamp.valueOf(x.toLocalDateTime))

  implicit val doobieLocalDateMeta: Meta[LocalDate] = Meta[Date].timap(_.toLocalDate)(Date.valueOf)

}
