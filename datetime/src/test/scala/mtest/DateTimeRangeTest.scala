package mtest

import cats.derived.auto.eq._
import cats.kernel.laws.discipline.UpperBoundedTests
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class DateTimeRangeTest extends AnyFunSuite with Discipline {

  implicit val arbiNJDateTimeRange: Arbitrary[NJDateTimeRange] =
    Arbitrary(for {
      start <- Gen.posNum[Long]
      inc <- Gen.posNum[Long]
      opts <- Gen.option(start).map(_.map(NJTimestamp(_)))
      opte <- Gen.option(start + inc).map(_.map(NJTimestamp(_)))
    } yield NJDateTimeRange(opts, opte))

  implicit val cogen: Cogen[NJDateTimeRange] = Cogen(m => m.start.map(_.milliseconds).getOrElse(0))

  checkAll("NJDateTimeRange", UpperBoundedTests[NJDateTimeRange].partialOrder)
}
