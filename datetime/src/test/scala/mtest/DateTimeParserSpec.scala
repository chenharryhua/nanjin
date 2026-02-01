package mtest

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.{DateTimeParser, FailedParsers}
import org.scalatest.funsuite.AnyFunSuite

import java.time.*
import java.time.format.DateTimeParseException

class DateTimeParserSpec extends AnyFunSuite {

  // --- LocalDate parser ---
  test("LocalDate parses ISO date string") {
    val parser = DateTimeParser.localDateParser
    val result = parser.parse("2026-02-01")
    assert(result.contains(LocalDate.of(2026, 2, 1)))
  }

  test("LocalDate parser fails on invalid string") {
    val parser = DateTimeParser.localDateParser
    val result = parser.parse("not-a-date")
    assert(result.isLeft)
    assert(result.left.exists(_.parsers.exists(_ == "LocalDate")))
  }

  // --- LocalTime parser ---
  test("LocalTime parses ISO time string") {
    val parser = DateTimeParser.localTimeParser
    val result = parser.parse("13:45:30")
    assert(result.contains(LocalTime.of(13, 45, 30)))
  }

  test("LocalTime parser fails on invalid string") {
    val parser = DateTimeParser.localTimeParser
    val result = parser.parse("25:99")
    assert(result.isLeft)
    assert(result.left.exists(_.parsers.exists(_ == "LocalTime")))
  }

  // --- LocalDateTime parser ---
  test("LocalDateTime parses ISO datetime string") {
    val parser = DateTimeParser.localDateTimeParser
    val result = parser.parse("2026-02-01T13:45:30")
    assert(result.contains(LocalDateTime.of(2026, 2, 1, 13, 45, 30)))
  }

  // --- Instant parser ---
  test("Instant parses ISO instant string") {
    val parser = DateTimeParser.instantParser
    val result = parser.parse("2026-02-01T13:45:30Z")
    assert(result.contains(Instant.parse("2026-02-01T13:45:30Z")))
  }

  // --- ZonedDateTime parser ---
  test("ZonedDateTime parses ISO string") {
    val parser = DateTimeParser.zonedParser
    val str = "2026-02-01T13:45:30+02:00[Europe/Berlin]"
    val result = parser.parse(str)
    assert(result.contains(ZonedDateTime.parse(str)))
  }

  // --- OffsetDateTime parser ---
  test("OffsetDateTime parses ISO string") {
    val parser = DateTimeParser.offsetParser
    val str = "2026-02-01T13:45:30+02:00"
    val result = parser.parse(str)
    assert(result.contains(OffsetDateTime.parse(str)))
  }

  // --- Alternative.combineK tests ---
  test("combineK tries second parser if first fails") {
    val p1 = DateTimeParser.localDateParser
    val p2 = DateTimeParser.localDateTimeParser
      .asInstanceOf[DateTimeParser[LocalDate]] // not meaningful, just to test combineK
    val combined = p1 <+> p2 // combining same parser for simplicity

    val r1 = combined.parse("not-a-date")
    assert(r1.isLeft)

    val r2 = combined.parse("2026-02-01")
    assert(r2.contains(LocalDate.of(2026, 2, 1)))
  }

  test("combineK aggregates failures from both parsers") {
    val p1 = DateTimeParser.localDateParser
    val p2 = DateTimeParser.localTimeParser
      .asInstanceOf[DateTimeParser[LocalDate]] // forced cast to test failure aggregation
    val combined = p1 <+> p2

    val r = combined.parse("invalid")
    assert(r.isLeft)
    val failures = r.swap.toOption.get.parsers.toList
    assert(failures.contains("LocalDate"))
    assert(failures.contains("LocalTime"))
  }

  // --- Alternative.ap tests ---
  test("ap applies function parser to value parser") {
    val alt = DateTimeParser.alternativeDateTimeParser
    val funcParser: DateTimeParser[Int => Int] = alt.pure((x: Int) => x + 1)
    val valueParser: DateTimeParser[Int] = alt.pure(41)
    val applied = funcParser.ap(valueParser)
    val r = applied.parse("")
    assert(r.contains(42))
  }

  // --- FailedParsers parseException test ---
  test("FailedParsers produces meaningful exception") {
    val fp = FailedParsers("LocalDate")
    val ex = fp.parseException("abc")
    assert(ex.isInstanceOf[DateTimeParseException])
  }
}
