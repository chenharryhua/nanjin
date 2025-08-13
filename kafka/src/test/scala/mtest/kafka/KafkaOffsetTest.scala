package mtest.kafka

import cats.kernel.laws.discipline.{OrderTests, PartialOrderTests}
import cats.tests.CatsSuite
import com.github.chenharryhua.nanjin.kafka.{Offset, OffsetRange, Partition, TopicPartitionMap}
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class KafkaOffsetTest extends CatsSuite with FunSuiteDiscipline {

  implicit val arbOffset: Arbitrary[Offset] = Arbitrary(Gen.choose[Long](0, Long.MaxValue).map(Offset(_)))

  implicit val cogen: Cogen[Offset] =
    Cogen[Offset]((o: Offset) => o.value)

  implicit val cogenRange: Cogen[OffsetRange] =
    Cogen[OffsetRange]((o: OffsetRange) => o.from)

  implicit val arbRange: Arbitrary[OffsetRange] = Arbitrary(
    for {
      os <- Gen.choose[Long](0, Long.MaxValue / 2).map(Offset(_))
      os2 <- Gen.choose[Long](1, Long.MaxValue / 2)
    } yield OffsetRange(os, Offset(os.value + os2)).get
  )

  implicit val cogenPartition: Cogen[Partition] =
    Cogen[Partition]((p: Partition) => p.value.toLong)

  implicit val arbPartition: Arbitrary[Partition] = Arbitrary(
    Gen.choose[Int](0, Int.MaxValue).map(Partition(_)))

  checkAll("kafka offset", OrderTests[Offset].order)
  checkAll("kafka partition", OrderTests[Partition].order)
  checkAll("kafka offset range", PartialOrderTests[OffsetRange].partialOrder)
}

class KafkaOffsetBuildTest extends AnyFunSuite {

  test("partition") {
    val p1 = Partition(1)
    val p2 = Partition(2)
    assert(p2 - p1 == 1)
  }

  test("KafkaTopicPartition empty") {
    val ktp = TopicPartitionMap.empty[Int]
    assert(ktp.isEmpty)
    assert(!ktp.nonEmpty)
  }

  test("KafkaTopicPartition") {
    val ktp: TopicPartitionMap[Option[OffsetAndTimestamp]] =
      TopicPartitionMap[Option[OffsetAndTimestamp]](
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
    assert(ktp.mapValues(_.map(Offset(_))).value.values.size == 3)
    assert(ktp.mapValues(_.map(Offset(_))).flatten.value.values.toList.map(_.value).toSet == Set(0, 1))
  }

  test("intersect combine") {
    val k1 = TopicPartitionMap[Int](
      Map(
        new TopicPartition("topic", 0) -> 0,
        new TopicPartition("topic", 2) -> 2
      ))
    val k2 = TopicPartitionMap[Int](
      Map(
        new TopicPartition("topic", 0) -> 0,
        new TopicPartition("topic", 1) -> 1
      ))

    val res = k1.intersectCombine(k2)((r, l) => r + l).value
    assert(res.size == 1)
    assert(res.get(new TopicPartition("topic", 0)).contains(0))
  }

  test("left combine") {
    val k1 = TopicPartitionMap[Int](
      Map(
        new TopicPartition("topic", 0) -> 0,
        new TopicPartition("topic", 2) -> 2
      ))
    val k2 = TopicPartitionMap[Int](
      Map(
        new TopicPartition("topic", 0) -> 0,
        new TopicPartition("topic", 1) -> 1
      ))

    val res = k1.leftCombine(k2)((l, r) => Some(l + r)).value
    assert(res.size == 2)
    assert(res(new TopicPartition("topic", 0)).contains(0))
    assert(res(new TopicPartition("topic", 2)).isEmpty)
  }

  test("empty offset") {
    assert(TopicPartitionMap.emptyOffset.value.isEmpty)
  }
}
