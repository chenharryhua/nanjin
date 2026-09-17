package com.github.chenharryhua.nanjin.kafka.admins

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.admins.SnapshotConsumer
import com.github.chenharryhua.nanjin.kafka.buildConsumer.*
import com.github.chenharryhua.nanjin.kafka.{buildConsumer, Offset, TopicPartitionMap}
import fs2.kafka.consumer.MkConsumer
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Instant, LocalDate}
import com.github.chenharryhua.nanjin.kafka.PureConsumerSettings

class TransientConsumerTest extends AnyFunSuite {
  private val pcs = PureConsumerSettings
  test("1.offsetRangeFor - 1") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 10)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res = consumer.flatMap(_.offsetRangeFor(DateTimeRange(sydneyTime))).unsafeRunSync()
    assert(res.nonEmpty)
    assert(res.treeMap.size == 3)
    assert(res.treeMap.forall(_._2.forall(_.distance == 10)))
  }

  test("2.offsetRangeFor - 2") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map(
      tp0 -> new OffsetAndTimestamp(5, 0),
      tp1 -> new OffsetAndTimestamp(5, 0),
      tp2 -> new OffsetAndTimestamp(5, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res = consumer.flatMap(_.offsetRangeFor(DateTimeRange(sydneyTime))).unsafeRunSync()
    assert(res.treeMap.size == 3)
    assert(res.treeMap.forall(_._2.forall(_.distance == 10)))
  }

  test("3.offsetRangeFor - 3") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res =
      consumer
        .flatMap(_.offsetRangeFor(DateTimeRange(sydneyTime).withEndTime(LocalDate.now())))
        .unsafeRunSync()
    assert(res.treeMap.size == 3)
    assert(res.treeMap.forall(_._2.exists(_.distance == 5)))
  }

  test("4.offsetRangeFor - 4") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map(tp0 -> null, tp1 -> null, tp2 -> null)
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res =
      consumer
        .flatMap(_.offsetRangeFor(DateTimeRange(sydneyTime).withEndTime(LocalDate.now())))
        .unsafeRunSync()
    assert(res.nonEmpty)
    assert(res.treeMap.size == 3)
    assert(res.treeMap.forall(_._2.exists(_.distance == 10)))
  }

  test("5.offsetRangeFor - 5") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> null, tp1 -> null, tp2 -> null)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res =
      consumer
        .flatMap(_.offsetRangeFor(DateTimeRange(sydneyTime).withEndTime(LocalDate.now())))
        .unsafeRunSync()
    assert(res.nonEmpty)
    assert(res.treeMap.size == 3)
    assert(res.treeMap.forall(_._2.exists(_.distance == 5)))
  }

  test("6.offset for time") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res = consumer.flatMap(_.offsetsForTimes(Instant.ofEpochSecond(1))).unsafeRunSync()
    val expected = TopicPartitionMap(forTime.map { case (tp, of) => tp -> Option(Offset(of)) })

    assert(res.treeMap.size == 3)
    assert(res == expected)
  }

  test("7.coverage") {
    val begin: Map[TopicPartition, java.lang.Long] = Map.empty
    val end: Map[TopicPartition, java.lang.Long] = Map.empty
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map.empty
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap(_.commitSync(Map.empty)).unsafeRunSync()
  }

  test("8.partitionsFor lists every partition of the topic") {
    val mkConsumer: MkConsumer[IO] = buildConsumer(Map.empty, Map.empty, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res = consumer.flatMap(_.partitionsFor).unsafeRunSync()
    assert(res.toSet == Set(tp0, tp1, tp2))
  }

  test("9.beginningOffsets and endOffsets map raw offsets to Option[Offset]") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 3, tp2 -> 7)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))

    val beg = consumer.flatMap(_.beginningOffsets).unsafeRunSync()
    assert(beg.get(tp0).contains(Some(Offset(0))))
    assert(beg.get(tp1).contains(Some(Offset(3))))
    assert(beg.get(tp2).contains(Some(Offset(7))))

    val fin = consumer.flatMap(_.endOffsets).unsafeRunSync()
    assert(fin.treeMap.size == 3)
    assert(fin.treeMap.forall(_._2.contains(Offset(10))))
  }

  test("10.a null end offset surfaces as Some(None)") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    // tp2 present but null -> the Kafka client reports the offset as unavailable -> Some(None)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> null)
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res = consumer.flatMap(_.endOffsets).unsafeRunSync()
    assert(res.get(tp0).contains(Some(Offset(10))))
    assert(res.get(tp2).contains(None))
  }

  test("11.offsetRangeFor(start, end) uses offsetsForTimes on both ends") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    // both start and end resolve to the same offsetsForTimes result; distance is 0
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res =
      consumer
        .flatMap(_.offsetRangeFor(Instant.ofEpochSecond(1), Instant.ofEpochSecond(2)))
        .unsafeRunSync()
    assert(res.treeMap.size == 3)
    // from == until (both 5) -> OffsetRange.apply returns None
    assert(res.treeMap.forall(_._2.isEmpty))
  }

  test("12.offsetRangeForAll spans beginning to end") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 2, tp2 -> 4)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res = consumer.flatMap(_.offsetRangeForAll).unsafeRunSync()
    assert(res.get(tp0).flatten.map(_.distance).contains(10))
    assert(res.get(tp1).flatten.map(_.distance).contains(8))
    assert(res.get(tp2).flatten.map(_.distance).contains(6))
  }

  test("13.offsetRangeSince spans offsetsForTimes to end") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val res = consumer.flatMap(_.offsetRangeSince(Instant.ofEpochSecond(1))).unsafeRunSync()
    assert(res.treeMap.size == 3)
    assert(res.treeMap.forall(_._2.exists(_.distance == 5)))
  }

  test("14.resetOffsets variants run against the consumer without error") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    val run = consumer.flatMap { c =>
      c.resetOffsetsToBegin >> c.resetOffsetsToEnd >> c.resetOffsetsForTimes(Instant.ofEpochSecond(1))
    }
    run.unsafeRunSync()
  }

  test("15.resetOffsetsToEnd tolerates a missing end offset") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    // tp2 missing -> None -> dropped by offsetsOf before committing
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10)
    val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer = mkConsumer(pcs).map(SnapshotConsumer[IO](topicName, _))
    consumer.flatMap(_.resetOffsetsToEnd).unsafeRunSync()
  }
}
