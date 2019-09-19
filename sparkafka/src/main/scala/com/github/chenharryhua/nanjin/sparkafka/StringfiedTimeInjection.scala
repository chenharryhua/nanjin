package com.github.chenharryhua.nanjin.sparkafka

import java.time.{Instant, LocalDateTime, ZonedDateTime}

import frameless.Injection

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
}
