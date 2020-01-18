package mtest

import cats.derived.auto.eq._
import cats.kernel.laws.discipline.UpperBoundedTests
import com.github.chenharryhua.nanjin.datetime._
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import org.scalatest.prop.Configuration
import shapeless.syntax.inject._
import java.time.{LocalDateTime, ZoneId}
class DateTimeRangeTest extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  implicit val arbiNJDateTimeRange: Arbitrary[NJDateTimeRange] =
    Arbitrary(for {
      start <- Gen.posNum[Long]
      inc <- Gen.posNum[Long]
      opts <- Gen.option(start).map(_.map(NJTimestamp(_).inject[NJDateTimeRange.TimeTypes]))
      opte <- Gen.option(start + inc).map(_.map(NJTimestamp(_).inject[NJDateTimeRange.TimeTypes]))
    } yield NJDateTimeRange(opts, opte, ZoneId.systemDefault()))

  implicit val cogen: Cogen[NJDateTimeRange] =
    Cogen(m => m.startTimestamp.map(_.milliseconds).getOrElse(0))

  checkAll("NJDateTimeRange", UpperBoundedTests[NJDateTimeRange].upperBounded)


    test("order of applying time data does not matter") {
      val zoneId    = ZoneId.of("Asia/Chongqing")
      val startTime = LocalDateTime.of(2012, 10, 26, 18, 0, 0)
      val endTime   = LocalDateTime.of(2012, 10, 26, 23, 0, 0)

      val param = NJDateTimeRange.infinite

      val a = param.withEnd(endTime).withZoneId(zoneId).withStart(startTime)
      val b = param.withStart(startTime).withZoneId(zoneId).withEnd(endTime)

      assert(a === b)
    }
}
