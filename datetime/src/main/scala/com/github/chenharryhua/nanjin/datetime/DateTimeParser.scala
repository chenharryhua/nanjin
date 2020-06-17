package com.github.chenharryhua.nanjin.datetime

import java.text.SimpleDateFormat
import java.time._

import cats.Alternative
import cats.implicits._

trait DateTimeParser[A] extends Serializable { self =>
  def parse(str: String): Either[Throwable, A]
}

object DateTimeParser {
  def apply[A](implicit ev: DateTimeParser[A]): DateTimeParser[A] = ev

  implicit val localDateParser: DateTimeParser[LocalDate] = (str: String) =>
    Either.catchNonFatal(LocalDate.parse(str))

  implicit val localTimeParser: DateTimeParser[LocalTime] = (str: String) =>
    Either.catchNonFatal(LocalTime.parse(str))

  implicit val localDateTimeParser: DateTimeParser[LocalDateTime] = (str: String) =>
    Either.catchNonFatal(LocalDateTime.parse(str))

  implicit val instantParser: DateTimeParser[Instant] = (str: String) =>
    Either.catchNonFatal(Instant.parse(str))

  implicit val zonedParser: DateTimeParser[ZonedDateTime] = (str: String) =>
    Either.catchNonFatal(ZonedDateTime.parse(str))

  implicit val offsetParser: DateTimeParser[OffsetDateTime] = (str: String) =>
    Either.catchNonFatal(OffsetDateTime.parse(str))

  implicit val customerizedParser: DateTimeParser[NJTimestamp] = { (str: String) =>
    val fmt = new SimpleDateFormat("yyyyMMdd")
    Either.catchNonFatal(NJTimestamp(fmt.parse(str).toInstant))
  }

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
