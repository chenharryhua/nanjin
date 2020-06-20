package mtest

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}

import cats.kernel.laws.discipline.{PartialOrderTests, UpperBoundedTests}
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.genZonedDateTimeWithZone
import com.github.chenharryhua.nanjin.datetime._
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import cats.implicits._

class DateTimeRangeTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  implicit val arbiNJDateTimeRange: Arbitrary[NJDateTimeRange] =
    Arbitrary(for {
      date <- genZonedDateTimeWithZone(None)
      inc <- Gen.posNum[Long]
      d = date.toLocalDateTime
    } yield NJDateTimeRange.infinite.withStartTime(d).withEndTime(d.plusSeconds(inc)))

  implicit val cogen: Cogen[NJDateTimeRange] =
    Cogen(m => m.startTimestamp.map(_.milliseconds).getOrElse(0))

  checkAll("NJDateTimeRange-UpperBounded", UpperBoundedTests[NJDateTimeRange].upperBounded)
  checkAll("NJDateTimeRange-PartialOrder", PartialOrderTests[NJDateTimeRange].partialOrder)

  test("order of applying time data does not matter") {
    val zoneId    = ZoneId.of("Asia/Chongqing")
    val startTime = LocalDateTime.of(2012, 10, 26, 18, 0, 0)
    val endTime   = LocalDateTime.of(2012, 10, 26, 23, 0, 0)

    val param = NJDateTimeRange.infinite

    val a = param.withEndTime(endTime).withZoneId(zoneId).withStartTime(startTime)
    val b = param.withStartTime(startTime).withZoneId(zoneId).withEndTime(endTime)
    val c = param.withZoneId(zoneId).withStartTime(startTime).withEndTime(endTime)
    val d = param.withEndTime(endTime).withStartTime(startTime).withZoneId(zoneId)
    val e = param
      .withEndTime("2012-10-26T23:00:00")
      .withStartTime("2012-10-26T18:00:00")
      .withZoneId(zoneId)

    assert(a.eqv(b))
    assert(a.eqv(c))
    assert(a.eqv(d))
    assert(a.eqv(e))
    assert(a.zonedStartTime.get.eqv(startTime.atZone(zoneId)))
    assert(a.zonedEndTime.get.eqv(endTime.atZone(zoneId)))
  }
  test("days should return list of date from start(inclusive) to end(exclusive)") {
    val d1 = LocalDate.of(2012, 10, 26)
    val d2 = LocalDate.of(2012, 10, 27)
    val d3 = LocalDate.of(2012, 10, 28)

    val dtr = NJDateTimeRange.infinite.withStartTime(d1).withEndTime(d3)

    assert(dtr.days.eqv(List(d1, d2)))

    assert(NJDateTimeRange.oneDay(d3).days.eqv(List(d3)))
  }

  test("infinite range should return empty list") {
    assert(NJDateTimeRange.infinite.days.isEmpty)
  }

  test("days of same day should return empty list") {
    val d3  = LocalDate.of(2012, 10, 28)
    val dt4 = LocalDateTime.of(d3, LocalTime.of(10, 1, 1))
    val dt5 = LocalDateTime.of(d3, LocalTime.of(10, 1, 2))

    val sameDay = NJDateTimeRange.infinite.withStartTime(dt4).withEndTime(dt5)
    assert(sameDay.days.isEmpty)
  }
}
