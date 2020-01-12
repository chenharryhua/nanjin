package mtest.kafka

import cats.kernel.laws.discipline.PartialOrderTests
import com.github.chenharryhua.nanjin.kafka.{KafkaOffset, KafkaOffsetRange}
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import cats.derived.auto.eq._

class LawTests extends AnyFunSuite with Discipline {

  implicit val arbiKafkaOffsetRange: Arbitrary[KafkaOffsetRange] =
    Arbitrary(for {
      from <- Gen.posNum[Long]
      inc <- Gen.posNum[Long]
    } yield KafkaOffsetRange(KafkaOffset(from), KafkaOffset(from + inc)))

  implicit val cogen: Cogen[KafkaOffsetRange] = Cogen(m => m.distance)

  checkAll("KafkaOffsetRange", PartialOrderTests[KafkaOffsetRange].partialOrder)
}
