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
    Arbitrary(for {
      s <- Gen.posNum[Long]
      inc <- Gen.posNum[Long]
      opts <- Gen.option(s).map(_.map(NJTimestamp(_)))
      opte <- Gen.option(s + inc).map(_.map(NJTimestamp(_)))
    } yield NJDateTimeRange(opts, opte))

  implicit val cogen: Cogen[NJDateTimeRange] = Cogen(m => m.start.map(_.milliseconds).getOrElse(0))

  checkAll("NJDateTimeRange", UpperBoundedTests[NJDateTimeRange].partialOrder)
}
