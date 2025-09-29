package com.github.chenharryhua.nanjin.datetime

import cats.Alternative
import cats.data.NonEmptyList
import cats.syntax.all.*

import java.time.*
import java.time.format.DateTimeParseException

final case class FailedParsers(parsers: NonEmptyList[String]) extends AnyVal {
  def concat(other: FailedParsers): FailedParsers = FailedParsers(parsers ::: other.parsers)

  def parseException(str: String): DateTimeParseException =
    new DateTimeParseException(
      s"""can not parse "$str" by any of [${parsers.toList.mkString(",")}]""",
      str,
      -1)
}

object FailedParsers {
  def apply(parser: String): FailedParsers = FailedParsers(NonEmptyList.one(parser))
}

sealed trait DateTimeParser[A] { self =>
  def parse(str: String): Either[FailedParsers, A]
}

object DateTimeParser {
  def apply[A](implicit ev: DateTimeParser[A]): DateTimeParser[A] = ev

  implicit final val localDateParser: DateTimeParser[LocalDate] =
    new DateTimeParser[LocalDate] {

      override def parse(str: String): Either[FailedParsers, LocalDate] =
        Either.catchNonFatal(LocalDate.parse(str)).leftMap(_ => FailedParsers("LocalDate"))
    }

  implicit final val localTimeParser: DateTimeParser[LocalTime] =
    new DateTimeParser[LocalTime] {

      override def parse(str: String): Either[FailedParsers, LocalTime] =
        Either.catchNonFatal(LocalTime.parse(str)).leftMap(_ => FailedParsers("LocalTime"))
    }

  implicit final val localDateTimeParser: DateTimeParser[LocalDateTime] =
    new DateTimeParser[LocalDateTime] {

      override def parse(str: String): Either[FailedParsers, LocalDateTime] =
        Either.catchNonFatal(LocalDateTime.parse(str)).leftMap(_ => FailedParsers("LocalDateTime"))
    }

  implicit final val instantParser: DateTimeParser[Instant] =
    new DateTimeParser[Instant] {

      override def parse(str: String): Either[FailedParsers, Instant] =
        Either.catchNonFatal(Instant.parse(str)).leftMap(_ => FailedParsers("Instant"))
    }

  implicit final val zonedParser: DateTimeParser[ZonedDateTime] =
    new DateTimeParser[ZonedDateTime] {

      override def parse(str: String): Either[FailedParsers, ZonedDateTime] =
        Either.catchNonFatal(ZonedDateTime.parse(str)).leftMap(_ => FailedParsers("ZonedDateTime"))
    }

  implicit final val offsetParser: DateTimeParser[OffsetDateTime] =
    new DateTimeParser[OffsetDateTime] {

      override def parse(str: String): Either[FailedParsers, OffsetDateTime] =
        Either.catchNonFatal(OffsetDateTime.parse(str)).leftMap(_ => FailedParsers("OffsetDateTime"))
    }

  implicit final val alternativeDateTimeParser: Alternative[DateTimeParser] =
    new Alternative[DateTimeParser] {

      override def empty[A]: DateTimeParser[A] =
        new DateTimeParser[A] {

          override def parse(str: String): Left[FailedParsers, A] =
            Left(FailedParsers("NonParser"))
        }

      override def combineK[A](x: DateTimeParser[A], y: DateTimeParser[A]): DateTimeParser[A] =
        new DateTimeParser[A] {

          override def parse(str: String): Either[FailedParsers, A] =
            x.parse(str) match {
              case r @ Right(_) => r
              case Left(ex)     =>
                y.parse(str) match {
                  case r @ Right(_) => r
                  case Left(ex2)    => Left(ex.concat(ex2))
                }
            }
        }

      override def pure[A](x: A): DateTimeParser[A] =
        new DateTimeParser[A] {
          override def parse(str: String): Either[FailedParsers, A] = Right(x)
        }

      override def ap[A, B](ff: DateTimeParser[A => B])(fa: DateTimeParser[A]): DateTimeParser[B] =
        new DateTimeParser[B] {

          override def parse(str: String): Either[FailedParsers, B] =
            fa.parse(str).ap(ff.parse(str))
        }
    }
}
