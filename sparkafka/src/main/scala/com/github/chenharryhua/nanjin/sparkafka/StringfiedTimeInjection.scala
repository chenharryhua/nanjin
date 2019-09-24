package com.github.chenharryhua.nanjin.sparkafka

import java.time.{Instant, LocalDateTime, ZonedDateTime}

import frameless.Injection
import java.time.LocalDate
import java.sql.{Date, Timestamp}

import doobie.util.Meta

object StringfiedTimeInjection {

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

  implicit object localDatejection extends Injection[LocalDate, String] {
    override def apply(a: LocalDate): String  = a.toString
    override def invert(b: String): LocalDate = LocalDate.parse(b)
  }

  implicit val doobieInstantMeta: Meta[Instant] = Meta[Timestamp].timap(_.toInstant)(Timestamp.from)
  implicit val doobieLocalDateTimeMeta: Meta[LocalDateTime] =
    Meta[Timestamp].timap(_.toLocalDateTime)(Timestamp.valueOf)
  implicit val doobieLocalDateMeta: Meta[LocalDate] = Meta[Date].timap(_.toLocalDate)(Date.valueOf)

}
