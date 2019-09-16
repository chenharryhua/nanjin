package com.github.chenharryhua.nanjin.sparkafka

import java.time.{Instant, LocalDateTime, ZonedDateTime}

import frameless.Injection

object StringfiedTimeInjection {
  implicit val instantInjection: Injection[Instant, String] =
    new Injection[Instant, String] {
      override def apply(a: Instant): String  = a.toString
      override def invert(b: String): Instant = Instant.parse(b)
    }
  implicit val localDateTimeInjection: Injection[LocalDateTime, String] =
    new Injection[LocalDateTime, String] {
      override def apply(a: LocalDateTime): String  = a.toString
      override def invert(b: String): LocalDateTime = LocalDateTime.parse(b)
    }
  implicit val zonedDateTimeInjection: Injection[ZonedDateTime, String] =
    new Injection[ZonedDateTime, String] {
      override def apply(a: ZonedDateTime): String  = a.toString
      override def invert(b: String): ZonedDateTime = ZonedDateTime.parse(b)
    }
}
