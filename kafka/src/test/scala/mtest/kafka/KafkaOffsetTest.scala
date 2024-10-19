package mtest.kafka

import cats.kernel.laws.discipline.{OrderTests, PartialOrderTests}
import cats.tests.CatsSuite
import com.github.chenharryhua.nanjin.kafka.{
  KafkaOffset,
  KafkaOffsetRange,
  KafkaPartition,
  KafkaTopicPartition
}
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

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
    } yield KafkaOffsetRange(os, KafkaOffset(os.value + os2)).get
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

  test("partition") {
    val p1 = KafkaPartition(1)
    val p2 = KafkaPartition(2)
    assert(p2 - p1 == 1)
  }

  test("KafkaTopicPartition empty") {
    val ktp = KafkaTopicPartition[Int](Map.empty)
    assert(ktp.isEmpty)
    assert(!ktp.nonEmpty)
  }

  test("KafkaTopicPartition") {
    val ktp: KafkaTopicPartition[Option[OffsetAndTimestamp]] =
      KafkaTopicPartition[Option[OffsetAndTimestamp]](
        Map(
          new TopicPartition("topic", 0) -> Some(new OffsetAndTimestamp(0, 1000)),
          new TopicPartition("topic", 1) -> Some(new OffsetAndTimestamp(1, 2000)),
          new TopicPartition("topic", 2) -> None
        ))
    val expected = Map(
      new TopicPartition("topic", 0) -> Some(1000),
      new TopicPartition("topic", 1) -> Some(2000),
      new TopicPartition("topic", 2) -> None
    )
    val res = ktp.map((_, v) => v.map(_.timestamp()))

    assert(res.value == expected)
    assert(ktp.topicPartitions.value.toSet == expected.keySet)
    assert(ktp.offsets.value.values.size == 3)
    assert(ktp.offsets.value.values.toList.flatten.map(_.value).toSet == Set(0, 1))
  }

  test("intersect combine") {
    val k1 = KafkaTopicPartition[Int](
      Map(
        new TopicPartition("topic", 0) -> 0,
        new TopicPartition("topic", 2) -> 2
      ))
    val k2 = KafkaTopicPartition[Int](
      Map(
        new TopicPartition("topic", 0) -> 0,
        new TopicPartition("topic", 1) -> 1
      ))

    val res = k1.intersectCombine(k2)((r, l) => r + l).value
    assert(res.size == 1)
    assert(res.get(new TopicPartition("topic", 0)).contains(0))
  }

  test("left combine") {
    val k1 = KafkaTopicPartition[Int](
      Map(
        new TopicPartition("topic", 0) -> 0,
        new TopicPartition("topic", 2) -> 2
      ))
    val k2 = KafkaTopicPartition[Int](
      Map(
        new TopicPartition("topic", 0) -> 0,
        new TopicPartition("topic", 1) -> 1
      ))

    val res = k1.leftCombine(k2)((l, r) => Some(l + r)).value
    assert(res.size == 2)
    assert(res(new TopicPartition("topic", 0)).contains(0))
    assert(res(new TopicPartition("topic", 2)).isEmpty)
  }
}
