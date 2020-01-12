package mtest

import cats.derived.auto.eq._
import cats.implicits._
import cats.kernel.laws.discipline.UpperBoundedTests
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class DateTimeRangeTest extends AnyFunSuite with Discipline {

  implicit val arbiKafkaOffsetRange: Arbitrary[NJDateTimeRange] =
    Arbitrary(
      for {
        opts <- Gen.option(Gen.posNum[Long])
        optn <- Gen.option(Gen.posNum[Long])
      } yield NJDateTimeRange(
        opts.map(t => NJTimestamp(t)),
        (optn.flatMap(x => opts.map(y => x + y))).map(t => NJTimestamp(t))))

  implicit val cogen: Cogen[NJDateTimeRange] = Cogen(m => m.start.map(_.milliseconds).getOrElse(0))

  checkAll("NJDateTimeRange", UpperBoundedTests[NJDateTimeRange].partialOrder)
}
