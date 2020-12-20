package mtest.kafka

import cats.kernel.laws.discipline.{OrderTests, PartialOrderTests}
import cats.tests.CatsSuite
import com.github.chenharryhua.nanjin.kafka.{KafkaOffset, KafkaOffsetRange, KafkaPartition}
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.FunSuiteDiscipline
import cats.syntax.all._

class KafkaOffsetTest extends CatsSuite with FunSuiteDiscipline {

  implicit val arbOffset: Arbitrary[KafkaOffset] = Arbitrary(
    Gen.choose[Long](0, Long.MaxValue).map(KafkaOffset(_)))

  implicit val cogen: Cogen[KafkaOffset] =
    Cogen[KafkaOffset]((o: KafkaOffset) => o.value)

  implicit val cogenRange: Cogen[KafkaOffsetRange] =
    Cogen[KafkaOffsetRange]((o: KafkaOffsetRange) => o.from.value)

  implicit val arbRange: Arbitrary[KafkaOffsetRange] = Arbitrary(
    for {
      os <- Gen.choose[Long](0, Long.MaxValue / 2).map(KafkaOffset(_))
      os2 <- Gen.choose[Long](1, Long.MaxValue / 2)
    } yield KafkaOffsetRange(os, KafkaOffset(os.offset.value + os2)).get
  )

  implicit val cogenPartition: Cogen[KafkaPartition] =
    Cogen[KafkaPartition]((p: KafkaPartition) => p.value.toLong)

  implicit val arbPartition: Arbitrary[KafkaPartition] = Arbitrary(
    Gen.choose[Int](0, Int.MaxValue).map(KafkaPartition(_)))

  checkAll("kafka offset", OrderTests[KafkaOffset].order)
  checkAll("kafka partition", OrderTests[KafkaPartition].order)
  checkAll("kafka offset range", PartialOrderTests[KafkaOffsetRange].partialOrder)
}

class KafkaOffsetBuildTest extends AnyFunSuite {
  test("non negative KafkaOffset") {
    assertThrows[Exception](KafkaOffset(-1))
    assertThrows[Exception](KafkaPartition(-1))
  }
  test("kafka offset range") {
    assert(KafkaOffsetRange(KafkaOffset(100), KafkaOffset(99)).isEmpty)
    val r = KafkaOffsetRange(KafkaOffset(1), KafkaOffset(99)).get
    r.show
    assert(r.distance == 98)
  }
  test("partition") {
    val p1 = KafkaPartition(1)
    val p2 = KafkaPartition(2)
    assert(p2 - p1 == 1)
  }
}
