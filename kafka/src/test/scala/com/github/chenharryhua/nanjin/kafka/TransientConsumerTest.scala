package com.github.chenharryhua.nanjin.kafka

import cats.Id
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.{DateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.buildConsumer.*
import fs2.kafka.ConsumerSettings
import fs2.kafka.consumer.MkConsumer
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate

class TransientConsumerTest extends AnyFunSuite {
  private val pcs = ConsumerSettings[Id, Nothing, Nothing](null, null)
  test("offsetRangeFor - 1") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 10)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer  = SnapshotConsumer[IO](topicName, pcs)
    val res = consumer.use(_.offsetRangeFor(DateTimeRange(sydneyTime))).unsafeRunSync()
    println(res)
    assert(res.value.size == 3)
    assert(res.value.forall(_._2.forall(_.distance == 10)))
  }

  test("offsetRangeFor - 2") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map(
      tp0 -> new OffsetAndTimestamp(5, 0),
      tp1 -> new OffsetAndTimestamp(5, 0),
      tp2 -> new OffsetAndTimestamp(5, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer  = SnapshotConsumer[IO](topicName, pcs)
    val res = consumer.use(_.offsetRangeFor(DateTimeRange(sydneyTime))).unsafeRunSync()
    assert(res.value.size == 3)
    assert(res.value.forall(_._2.forall(_.distance == 10)))
  }

  test("offsetRangeFor - 3") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer  = SnapshotConsumer[IO](topicName, pcs)
    val res =
      consumer.use(_.offsetRangeFor(DateTimeRange(sydneyTime).withEndTime(LocalDate.now()))).unsafeRunSync()
    assert(res.value.size == 3)
    assert(res.value.forall(_._2.exists(_.distance == 5)))
  }

  test("offsetRangeFor - 4") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map(tp0 -> null, tp1 -> null, tp2 -> null)
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer  = SnapshotConsumer[IO](topicName, pcs)
    val res =
      consumer.use(_.offsetRangeFor(DateTimeRange(sydneyTime).withEndTime(LocalDate.now()))).unsafeRunSync()
    println(res)
    assert(res.value.size == 3)
    assert(res.value.forall(_._2.exists(_.distance == 10)))
  }

  test("offsetRangeFor - 5") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> null, tp1 -> null, tp2 -> null)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer  = SnapshotConsumer[IO](topicName, pcs)
    val res =
      consumer.use(_.offsetRangeFor(DateTimeRange(sydneyTime).withEndTime(LocalDate.now()))).unsafeRunSync()
    println(res)
    assert(res.value.size == 3)
    assert(res.value.forall(_._2.exists(_.distance == 5)))
  }

  test("offset for time") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(
        tp0 -> new OffsetAndTimestamp(5, 0),
        tp1 -> new OffsetAndTimestamp(5, 0),
        tp2 -> new OffsetAndTimestamp(5, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer = SnapshotConsumer[IO](topicName, pcs)

    val res = consumer.use(_.offsetsForTimes(NJTimestamp(1))).unsafeRunSync()
    val expected = TopicPartitionMap(forTime.map { case (tp, of) => tp -> Option(Offset(of)) })

    assert(res.value.size == 3)
    assert(res == expected)
  }

  test("coverage") {
    val begin: Map[TopicPartition, java.lang.Long] = Map.empty
    val end: Map[TopicPartition, java.lang.Long] = Map.empty
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map.empty
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer  = SnapshotConsumer[IO](topicName, pcs)
    consumer.use(_.commitSync(Map.empty)).unsafeRunSync()
  }
}
