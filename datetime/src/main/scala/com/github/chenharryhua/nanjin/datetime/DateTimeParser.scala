package com.github.chenharryhua.nanjin.datetime

import java.time._

import cats.Alternative
import cats.data.NonEmptyList
import cats.implicits._

sealed trait DateTimeParser[A] extends Serializable { self =>
  def parse(str: String): Either[NonEmptyList[String], A]
}

object DateTimeParser {
  def apply[A](implicit ev: DateTimeParser[A]): DateTimeParser[A] = ev

  implicit val localDateParser: DateTimeParser[LocalDate] =
    new DateTimeParser[LocalDate] {

      override def parse(str: String): Either[NonEmptyList[String], LocalDate] =
        Either.catchNonFatal(LocalDate.parse(str)).leftMap(_ => NonEmptyList.one("LocalDate"))
    }

  implicit val localTimeParser: DateTimeParser[LocalTime] =
    new DateTimeParser[LocalTime] {

      override def parse(str: String): Either[NonEmptyList[String], LocalTime] =
        Either.catchNonFatal(LocalTime.parse(str)).leftMap(_ => NonEmptyList.one("LocalTime"))
    }

  implicit val localDateTimeParser: DateTimeParser[LocalDateTime] =
    new DateTimeParser[LocalDateTime] {

      override def parse(str: String): Either[NonEmptyList[String], LocalDateTime] =
        Either
          .catchNonFatal(LocalDateTime.parse(str))
          .leftMap(_ => NonEmptyList.one("LocalDateTime"))
    }

  implicit val instantParser: DateTimeParser[Instant] =
    new DateTimeParser[Instant] {

      override def parse(str: String): Either[NonEmptyList[String], Instant] =
        Either.catchNonFatal(Instant.parse(str)).leftMap(_ => NonEmptyList.one("Instant"))
    }

  implicit val zonedParser: DateTimeParser[ZonedDateTime] =
    new DateTimeParser[ZonedDateTime] {

      override def parse(str: String): Either[NonEmptyList[String], ZonedDateTime] =
        Either
          .catchNonFatal(ZonedDateTime.parse(str))
          .leftMap(_ => NonEmptyList.one("ZonedDateTime"))
    }

  implicit val offsetParser: DateTimeParser[OffsetDateTime] =
    new DateTimeParser[OffsetDateTime] {

      override def parse(str: String): Either[NonEmptyList[String], OffsetDateTime] =
        Either
          .catchNonFatal(OffsetDateTime.parse(str))
          .leftMap(_ => NonEmptyList.one("OffsetDateTime"))
    }

  implicit val alternativeDateTimeParser: Alternative[DateTimeParser] =
    new Alternative[DateTimeParser] {

      override def empty[A]: DateTimeParser[A] =
        new DateTimeParser[A] {

          override def parse(str: String): Left[NonEmptyList[String], A] =
            Left(NonEmptyList.one("NonParser"))
        }

      override def combineK[A](x: DateTimeParser[A], y: DateTimeParser[A]): DateTimeParser[A] =
        new DateTimeParser[A] {

          override def parse(str: String): Either[NonEmptyList[String], A] =
            x.parse(str) match {
              case r @ Right(_) => r
              case Left(ex) =>
                y.parse(str) match {
                  case r @ Right(_) => r
                  case Left(ex2)    => Left(ex ::: ex2)
                }
            }
        }

      override def pure[A](x: A): DateTimeParser[A] =
        new DateTimeParser[A] {
          override def parse(str: String): Either[NonEmptyList[String], A] = Right(x)
        }

      override def ap[A, B](ff: DateTimeParser[A => B])(fa: DateTimeParser[A]): DateTimeParser[B] =
        new DateTimeParser[B] {

          override def parse(str: String): Either[NonEmptyList[String], B] =
            fa.parse(str).ap(ff.parse(str))
        }
    }
}
