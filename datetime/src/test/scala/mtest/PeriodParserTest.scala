package mtest

import cats.data.Validated.Valid
import cats.data.{NonEmptyList, Validated}
import com.github.chenharryhua.nanjin.datetime.period
import munit.FunSuite

import java.time.{LocalDate, Period}

class PeriodParserTest extends FunSuite {
  val p7y: Validated[NonEmptyList[String], Period] = Valid(Period.parse("P7Y"))
  val p7y3d: Validated[NonEmptyList[String], Period] = Valid(Period.parse("P7Y3D"))
  val p5m: Validated[NonEmptyList[String], Period] = Valid(Period.parse("P5M"))
  val p7y5m3d: Validated[NonEmptyList[String], Period] = Valid(Period.parse("P7Y5M3D"))

  test("1.should return valid period") {
    assert(period("7years") == p7y)
    assert(period("5 month") == p5m)

    assert(period("7 years 5 months 3 days") == p7y5m3d)
    assert(period("7 year     5     month       3        day") == p7y5m3d)
    assert(period("7years5months3days") == p7y5m3d)
    assert(period("7 year  3days") == p7y3d)
    assert(period("7 year 5 month3 days") == p7y5m3d)
  }

  test("2.should return invalid when failed") {
    assert(period("7 month 5 month 3 days").isInvalid)
    assert(period("7 month 5 year 3 days").isInvalid)
  }

  test("3.s's") {
    assert(period("5 months") == p5m)
    assert(period("5 month") == p5m)
    assert(period("5 monthss").isInvalid)
  }

  test("4.should calculate the start date and respect leap year") {
    val today = LocalDate.of(2012, 10, 26)

    val p = period("2 years").toEither.toOption.get
    assert(today.minus(p) == LocalDate.of(2010, 10, 26))
    val p2 = period("2 years 3 month").toEither.toOption.get
    assert(today.minus(p2) == LocalDate.of(2010, 7, 26))
    val p3 = period("2 years 3 month 6 days").toEither.toOption.get
    assert(today.minus(p3) == LocalDate.of(2010, 7, 20))
  }

  test("5.standard ISO periods are preferred and surrounding whitespace is ignored") {
    val cases = List(
      "P1Y2M3D" -> Period.of(1, 2, 3),
      " P2W " -> Period.ofDays(14),
      "P0D" -> Period.ZERO,
      "P-1Y2M" -> Period.of(-1, 2, 0))

    cases.foreach { case (input, expected) =>
      assertEquals(period(input).toOption, Some(expected))
    }
  }

  test("6.homebrew grammar supports every ordered field combination") {
    val cases = List(
      "1 year 2 months 3 days" -> Period.of(1, 2, 3),
      "1 year 2 months" -> Period.of(1, 2, 0),
      "1 year 3 days" -> Period.of(1, 0, 3),
      "2 months 3 days" -> Period.of(0, 2, 3),
      "1 year" -> Period.ofYears(1),
      "2 months" -> Period.ofMonths(2),
      "3 days" -> Period.ofDays(3)
    )

    cases.foreach { case (input, expected) =>
      assertEquals(period(input).toOption, Some(expected))
    }
  }

  test("7.homebrew units are case-insensitive and accept singular/plural forms") {
    val cases = List(
      "1 YEAR" -> Period.ofYears(1),
      "2 Months" -> Period.ofMonths(2),
      "1 DaY" -> Period.ofDays(1),
      "2 YEARS 3 MONTHS 4 DAYS" -> Period.of(2, 3, 4))

    cases.foreach { case (input, expected) =>
      assertEquals(period(input).toOption, Some(expected))
    }
  }

  test("8.zero and large in-range homebrew values parse") {
    assertEquals(period("0 years 0 months 0 days").toOption, Some(Period.ZERO))
    assertEquals(period("2147483647 years").toOption, Some(Period.ofYears(Int.MaxValue)))
  }

  test("9.malformed homebrew input returns validation errors") {
    val invalid = List(
      "",
      "years",
      "one year",
      "1.5 years",
      "-1 years",
      "1 day 2 months",
      "1 month trailing",
      "1 year 2 years")

    invalid.foreach(input => assert(period(input).isInvalid, s"expected invalid period: $input"))
  }
}
