package com.github.chenharryhua.nanjin.datetime

import java.time._

import cats.Alternative
import cats.implicits._

import scala.util.Try

trait DateTimeParser[A] extends Serializable { self =>
  def parse(str: String): Either[Throwable, A]
}

object DateTimeParser {
  def apply[A](implicit ev: DateTimeParser[A]): DateTimeParser[A] = ev

  implicit val localDateParser: DateTimeParser[LocalDate] = (str: String) =>
    Try(LocalDate.parse(str)).toEither

  implicit val localDateTimeParser: DateTimeParser[LocalDateTime] = (str: String) =>
    Try(LocalDateTime.parse(str)).toEither

  implicit val instantParser: DateTimeParser[Instant] = (str: String) =>
    Try(Instant.parse(str)).toEither

  implicit val zonedParser: DateTimeParser[ZonedDateTime] = (str: String) =>
    Try(ZonedDateTime.parse(str)).toEither

  implicit val offsetParser: DateTimeParser[OffsetDateTime] = (str: String) =>
    Try(OffsetDateTime.parse(str)).toEither

  implicit val alternativeDateTimeParser: Alternative[DateTimeParser] =
    new Alternative[DateTimeParser] {
      override def empty[A]: DateTimeParser[A] = _ => Left(new Exception("unparsable"))

      override def combineK[A](x: DateTimeParser[A], y: DateTimeParser[A]): DateTimeParser[A] =
        (str: String) => x.parse(str).orElse(y.parse(str))

      override def pure[A](x: A): DateTimeParser[A] = (str: String) => Right(x)

      override def ap[A, B](ff: DateTimeParser[A => B])(fa: DateTimeParser[A]): DateTimeParser[B] =
        (str: String) => fa.parse(str).ap(ff.parse(str))
    }
}
