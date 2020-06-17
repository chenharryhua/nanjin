package com.github.chenharryhua.nanjin.datetime

import java.text.SimpleDateFormat
import java.time._

import cats.Alternative
import cats.implicits._

sealed trait DateTimeParser[A] extends Serializable { self =>
  def parse(str: String): Either[Throwable, A]
}

object DateTimeParser {
  def apply[A](implicit ev: DateTimeParser[A]): DateTimeParser[A] = ev

  implicit val localDateParser: DateTimeParser[LocalDate] = new DateTimeParser[LocalDate] {

    override def parse(str: String): Either[Throwable, LocalDate] =
      Either.catchNonFatal(LocalDate.parse(str))
  }

  implicit val localTimeParser: DateTimeParser[LocalTime] =
    new DateTimeParser[LocalTime] {

      override def parse(str: String): Either[Throwable, LocalTime] =
        Either.catchNonFatal(LocalTime.parse(str))
    }

  implicit val localDateTimeParser: DateTimeParser[LocalDateTime] =
    new DateTimeParser[LocalDateTime] {

      override def parse(str: String): Either[Throwable, LocalDateTime] =
        Either.catchNonFatal(LocalDateTime.parse(str))
    }

  implicit val instantParser: DateTimeParser[Instant] =
    new DateTimeParser[Instant] {

      override def parse(str: String): Either[Throwable, Instant] =
        Either.catchNonFatal(Instant.parse(str))
    }

  implicit val zonedParser: DateTimeParser[ZonedDateTime] =
    new DateTimeParser[ZonedDateTime] {

      override def parse(str: String): Either[Throwable, ZonedDateTime] =
        Either.catchNonFatal(ZonedDateTime.parse(str))
    }

  implicit val offsetParser: DateTimeParser[OffsetDateTime] =
    new DateTimeParser[OffsetDateTime] {

      override def parse(str: String): Either[Throwable, OffsetDateTime] =
        Either.catchNonFatal(OffsetDateTime.parse(str))
    }

  implicit val customerizedParser: DateTimeParser[NJTimestamp] =
    new DateTimeParser[NJTimestamp] {
      val fmt = new SimpleDateFormat("yyyyMMdd")

      override def parse(str: String): Either[Throwable, NJTimestamp] =
        Either
          .catchNonFatal(NJTimestamp(fmt.parse(str).toInstant))
          .leftMap(_ => new Exception(s"can not parse: $str"))
    }

  implicit val alternativeDateTimeParser: Alternative[DateTimeParser] =
    new Alternative[DateTimeParser] {

      override def empty[A]: DateTimeParser[A] =
        new DateTimeParser[A] {

          override def parse(str: String): Either[Throwable, A] =
            Left(new Exception("unparsable"))
        }

      override def combineK[A](x: DateTimeParser[A], y: DateTimeParser[A]): DateTimeParser[A] =
        new DateTimeParser[A] {
          override def parse(str: String): Either[Throwable, A] = x.parse(str).orElse(y.parse(str))
        }

      override def pure[A](x: A): DateTimeParser[A] =
        new DateTimeParser[A] {
          override def parse(str: String): Either[Throwable, A] = Right(x)
        }

      override def ap[A, B](ff: DateTimeParser[A => B])(fa: DateTimeParser[A]): DateTimeParser[B] =
        new DateTimeParser[B] {

          override def parse(str: String): Either[Throwable, B] =
            fa.parse(str).ap(ff.parse(str))
        }
    }
}
