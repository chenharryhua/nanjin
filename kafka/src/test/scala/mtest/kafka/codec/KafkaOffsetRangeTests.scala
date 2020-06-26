package mtest.kafka.codec

import cats.derived.auto.eq._
import cats.kernel.laws.discipline.{OrderTests, PartialOrderTests}
import com.github.chenharryhua.nanjin.kafka.{KafkaOffset, KafkaOffsetRange, KafkaPartition}
import com.github.chenharryhua.nanjin.kafka.common.KafkaOffsetRange
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import org.scalatest.prop.Configuration

class KafkaOffsetRangeTests extends AnyFunSuite with FunSuiteDiscipline with Configuration {

  implicit val arbiKafkaOffsetRange: Arbitrary[KafkaOffsetRange] =
    Arbitrary(for {
      from <- Gen.posNum[Long]
      inc <- Gen.posNum[Long]
    } yield (KafkaOffsetRange(KafkaOffset(from), KafkaOffset(from + inc))).get)

  implicit val arbiKafkaOffset: Arbitrary[KafkaOffset] =
    Arbitrary(Gen.posNum[Long].map(KafkaOffset(_)))

  implicit val arbiKafkaPartition: Arbitrary[KafkaPartition] =
    Arbitrary(Gen.posNum[Int].map(KafkaPartition(_)))

  implicit val cogen: Cogen[KafkaOffsetRange]     = Cogen(_.distance)
  implicit val coOffset: Cogen[KafkaOffset]       = Cogen(_.value)
  implicit val coPartition: Cogen[KafkaPartition] = Cogen(_.value.toLong)

  checkAll("KafkaOffsetRange", PartialOrderTests[KafkaOffsetRange].partialOrder)
  checkAll("KafkaOffset", OrderTests[KafkaOffset].order)
  checkAll("KafkaPartition", OrderTests[KafkaPartition].order)
}
