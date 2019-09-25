package com.github.chenharryhua.nanjin.sparkdb

import java.sql.{Date, Timestamp}
import java.time._

import doobie.util.Meta
import frameless.{Injection, SQLTimestamp}

object DatetimeInstances {
  private val zoneId: ZoneId = ZoneId.systemDefault()
//typed-spark
  implicit object instantInjection extends Injection[Instant, SQLTimestamp] {
    override def apply(a: Instant): SQLTimestamp  = SQLTimestamp(a.toEpochMilli)
    override def invert(b: SQLTimestamp): Instant = Instant.ofEpochMilli(b.us)
  }

  implicit object localDateTimeInjection extends Injection[LocalDateTime, Instant] {
    override def apply(a: LocalDateTime): Instant  = a.atZone(zoneId).toInstant
    override def invert(b: Instant): LocalDateTime = LocalDateTime.ofInstant(b, zoneId)
  }

  implicit object zonedDateTimeInjection extends Injection[ZonedDateTime, Instant] {
    override def apply(a: ZonedDateTime): Instant  = a.toInstant
    override def invert(b: Instant): ZonedDateTime = ZonedDateTime.ofInstant(b, zoneId)
  }

  implicit object localDateInjection extends Injection[LocalDate, Long] {
    override def apply(a: LocalDate): Long  = a.toEpochDay
    override def invert(b: Long): LocalDate = LocalDate.ofEpochDay(b)
  }

//doobie
  implicit val doobieInstantMeta: Meta[Instant] =
    Meta[Timestamp].timap(_.toInstant)(Timestamp.from)

  implicit val doobieLocalDateTimeMeta: Meta[LocalDateTime] =
    Meta[Timestamp].timap(_.toLocalDateTime)(Timestamp.valueOf)

  implicit val doobieZonedDateTimeMeta: Meta[ZonedDateTime] =
    Meta[Timestamp].timap(_.toLocalDateTime.atZone(zoneId))(x =>
      Timestamp.valueOf(x.toLocalDateTime))

  implicit val doobieLocalDateMeta: Meta[LocalDate] =
    Meta[Date].timap(_.toLocalDate)(Date.valueOf)
}
