package com.github.chenharryhua.nanjin.datetime

import cats.Alternative
import cats.data.NonEmptyList
import cats.syntax.either.given
import cats.syntax.apply.given

import java.time.*
import java.time.format.DateTimeParseException

/** The names of the parsers that failed to parse an input, accumulated so a combined parser can report every
  * alternative it tried.
  *
  * @param parserNames
  *   the failing parser names, most-recently-tried appended last
  */
final case class FailedParsers(parserNames: NonEmptyList[String]) extends AnyVal {

  /** Merges two failure sets, keeping this one's names first. */
  def concat(other: FailedParsers): FailedParsers = FailedParsers(parserNames ::: other.parserNames)

  /** Builds a `DateTimeParseException` for `str` whose message lists every attempted parser. */
  def parseException(str: String): DateTimeParseException =
    new DateTimeParseException(
      s"""can not parse "$str" by any of [${parserNames.toList.mkString(",")}]""",
      str,
      -1)
}

object FailedParsers {

  /** A single-name failure set. */
  def apply(parser: String): FailedParsers = FailedParsers(NonEmptyList.one(parser))
}

/** A typeclass for parsing a `String` into a date/time value of type `A`.
  *
  * Parsing returns `Either[FailedParsers, A]` rather than throwing, so failures carry the names of the
  * parsers that were tried. This is what lets instances be combined (via the `Alternative` instance) to
  * attempt several formats and report all of them on total failure.
  *
  * @tparam A
  *   the parsed value type
  */
sealed trait DateTimeParser[A] { self =>

  /** Parses `str`, yielding the value on success or the failing parser name(s) on failure. */
  def parse(str: String): Either[FailedParsers, A]
}

object DateTimeParser {

  /** Summons the `DateTimeParser` instance for `A`. */
  def apply[A](using ev: DateTimeParser[A]): DateTimeParser[A] = ev

  /** Parses a `LocalDate` via `LocalDate.parse`; failures are tagged `"LocalDate"`. */
  given DateTimeParser[LocalDate] =
    new DateTimeParser[LocalDate] {

      override def parse(str: String): Either[FailedParsers, LocalDate] =
        Either
          .catchOnly[DateTimeParseException](LocalDate.parse(str))
          .leftMap(_ => FailedParsers("LocalDate"))
    }

  /** Parses a `LocalTime` via `LocalTime.parse`; failures are tagged `"LocalTime"`. */
  given DateTimeParser[LocalTime] =
    new DateTimeParser[LocalTime] {

      override def parse(str: String): Either[FailedParsers, LocalTime] =
        Either
          .catchOnly[DateTimeParseException](LocalTime.parse(str))
          .leftMap(_ => FailedParsers("LocalTime"))
    }

  /** Parses a `LocalDateTime` via `LocalDateTime.parse`; failures are tagged `"LocalDateTime"`. */
  given DateTimeParser[LocalDateTime] =
    new DateTimeParser[LocalDateTime] {

      override def parse(str: String): Either[FailedParsers, LocalDateTime] =
        Either
          .catchOnly[DateTimeParseException](LocalDateTime.parse(str))
          .leftMap(_ => FailedParsers("LocalDateTime"))
    }

  /** Parses an `Instant` via `Instant.parse`; failures are tagged `"Instant"`. */
  given DateTimeParser[Instant] =
    new DateTimeParser[Instant] {

      override def parse(str: String): Either[FailedParsers, Instant] =
        Either.catchOnly[DateTimeParseException](Instant.parse(str)).leftMap(_ => FailedParsers("Instant"))
    }

  /** Parses a `ZonedDateTime` via `ZonedDateTime.parse`; failures are tagged `"ZonedDateTime"`. */
  given DateTimeParser[ZonedDateTime] =
    new DateTimeParser[ZonedDateTime] {

      override def parse(str: String): Either[FailedParsers, ZonedDateTime] =
        Either
          .catchOnly[DateTimeParseException](ZonedDateTime.parse(str))
          .leftMap(_ => FailedParsers("ZonedDateTime"))
    }

  /** Parses an `OffsetDateTime` via `OffsetDateTime.parse`; failures are tagged `"OffsetDateTime"`. */
  given DateTimeParser[OffsetDateTime] =
    new DateTimeParser[OffsetDateTime] {

      override def parse(str: String): Either[FailedParsers, OffsetDateTime] =
        Either
          .catchOnly[DateTimeParseException](OffsetDateTime.parse(str))
          .leftMap(_ => FailedParsers("OffsetDateTime"))
    }

  /** `Alternative` instance enabling parsers to be combined and run together.
    *
    *   - `empty`: always fails, tagged `"EmptyParser"`.
    *   - `combineK`: tries the first parser, falling back to the second; if both fail their failure names are
    *     concatenated.
    *   - `pure`: ignores the input and always succeeds with the given value.
    *   - `ap`: runs both parsers on the same input and applies the parsed function to the parsed argument.
    */
  given Alternative[DateTimeParser] =
    new Alternative[DateTimeParser] {

      override def empty[A]: DateTimeParser[A] =
        new DateTimeParser[A] {

          override def parse(str: String): Left[FailedParsers, A] =
            Left(FailedParsers("EmptyParser"))
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
            ff.parse(str).ap(fa.parse(str))
        }
    }
}
