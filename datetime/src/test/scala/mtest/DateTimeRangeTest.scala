package mtest

import java.time.{LocalDate, LocalDateTime, ZoneId}

import cats.derived.auto.eq._
import cats.kernel.laws.discipline.UpperBoundedTests
import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8.genZonedDateTimeWithZone
import com.github.chenharryhua.nanjin.datetime._
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.Configuration
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class DateTimeRangeTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  implicit val arbiNJDateTimeRange: Arbitrary[NJDateTimeRange] =
    Arbitrary(for {
      date <- genZonedDateTimeWithZone(None)
      inc <- Gen.posNum[Long]
      d = date.toLocalDateTime
    } yield NJDateTimeRange.infinite.withStartTime(d).withEndTime(d.plusSeconds(inc)))

  implicit val cogen: Cogen[NJDateTimeRange] =
    Cogen(m => m.startTimestamp.map(_.milliseconds).getOrElse(0))

  checkAll("NJDateTimeRange", UpperBoundedTests[NJDateTimeRange].upperBounded)

  test("order of applying time data does not matter") {
    val zoneId    = ZoneId.of("Asia/Chongqing")
    val startTime = LocalDateTime.of(2012, 10, 26, 18, 0, 0)
    val endTime   = LocalDateTime.of(2012, 10, 26, 23, 0, 0)

    val param = NJDateTimeRange.infinite

    val a = param.withEndTime(endTime).withZoneId(zoneId).withStartTime(startTime)
    val b = param.withStartTime(startTime).withZoneId(zoneId).withEndTime(endTime)
    val c = param.withZoneId(zoneId).withStartTime(startTime).withEndTime(endTime)
    val d = param.withEndTime(endTime).withStartTime(startTime).withZoneId(zoneId)

    assert(a === b)
    assert(a === c)
    assert(a === d)
    assert(a.zonedStartTime.get === startTime.atZone(zoneId))
    assert(a.zonedEndTime.get === endTime.atZone(zoneId))
  }
  test("days test") {
    val d1 = LocalDate.of(2012, 10, 26)
    val d2 = LocalDate.of(2012, 10, 27)
    val d3 = LocalDate.of(2012, 10, 28)

    val dtr = NJDateTimeRange.infinite.withStartTime(d1).withEndTime(d3)

    assert(dtr.days === List(d1, d2))

    assert(NJDateTimeRange.oneDay(d3).days === List(d3))
    assert(NJDateTimeRange.infinite.days === List())

  }

}
