package mtest

import cats.data.Validated.Valid
import cats.data.{NonEmptyList, Validated}
import com.github.chenharryhua.nanjin.datetime.period
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDate, Period}

class PeriodParserTest extends AnyFunSuite {
  val p7y: Validated[NonEmptyList[String], Period] = Valid(Period.parse("P7Y"))
  val p7y3d: Validated[NonEmptyList[String], Period] = Valid(Period.parse("P7Y3D"))
  val p5m: Validated[NonEmptyList[String], Period] = Valid(Period.parse("P5M"))
  val p7y5m3d: Validated[NonEmptyList[String], Period] = Valid(Period.parse("P7Y5M3D"))

  test("should return valid period") {
    assert(period("7years") == p7y)
    assert(period("5 month") == p5m)

    assert(period("7 years 5 months 3 days") == p7y5m3d)
    assert(period("7 year     5     month       3        day") == p7y5m3d)
    assert(period("7years5months3days") == p7y5m3d)
    assert(period("7 year  3days") == p7y3d)
    assert(period("7 year 5 month3 days") == p7y5m3d)
  }

  test("should return invalid when failed") {
    assert(period("7 month 5 month 3 days").isInvalid)
    assert(period("7 month 5 year 3 days").isInvalid)
  }

  test("s's") {
    assert(period("5 months") == p5m)
    assert(period("5 month") == p5m)
    assert(period("5 monthss").isInvalid)
  }

  test("should calculate the start date and respect leap year") {
    val today = LocalDate.of(2012, 10, 26)

    val Validated.Valid(p) = period("2 years")
    assert(today.minus(p) == LocalDate.of(2010, 10, 26))
    val Validated.Valid(p2) = period("2 years 3 month")
    assert(today.minus(p2) == LocalDate.of(2010, 7, 26))
    val Validated.Valid(p3) = period("2 years 3 month 6 days")
    assert(today.minus(p3) == LocalDate.of(2010, 7, 20))
  }
}
