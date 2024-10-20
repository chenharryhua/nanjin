package com.github.chenharryhua.nanjin.kafka

import cats.Id
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.buildConsumer.*
import fs2.kafka.ConsumerSettings
import fs2.kafka.consumer.MkConsumer
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

class TransientConsumerTest extends AnyFunSuite {
  private val pcs = ConsumerSettings[Id, Nothing, Nothing](null, null)
  test("offsetRangeFor - 1") {
    val begin: Map[TopicPartition, Long]    = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, Long]      = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    val consumer: TransientConsumer[IO]     = TransientConsumer[IO](topicName, pcs)
    val res = consumer.offsetRangeFor(NJDateTimeRange(sydneyTime)).unsafeRunSync()
    assert(res.value.forall(_._2.forall(_.distance == 10)))
  }

  test("offsetRangeFor - 2") {
    val begin: Map[TopicPartition, Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, Long]   = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map(
      tp0 -> new OffsetAndTimestamp(5, 0),
      tp1 -> new OffsetAndTimestamp(5, 0),
      tp2 -> new OffsetAndTimestamp(5, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer: TransientConsumer[IO]     = TransientConsumer[IO](topicName, pcs)
    val res = consumer.offsetRangeFor(NJDateTimeRange(sydneyTime)).unsafeRunSync()
    assert(res.value.forall(_._2.forall(_.distance == 10)))
  }

  test("offsetRangeFor - 3") {
    val begin: Map[TopicPartition, Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, Long]   = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] =
      Map(tp0 -> new OffsetAndTimestamp(5, 0), tp1 -> new OffsetAndTimestamp(5, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer: TransientConsumer[IO]     = TransientConsumer[IO](topicName, pcs)
    val res = consumer.offsetRangeFor(NJDateTimeRange(sydneyTime).withToday).unsafeRunSync()
    assert(res.value.forall(_._2.forall(_.distance == 5)))
  }

  test("offsetRangeFor - 4") {
    val begin: Map[TopicPartition, Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, Long]   = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    val forTime: Map[TopicPartition, OffsetAndTimestamp] = Map(
      tp0 -> new OffsetAndTimestamp(15, 0),
      tp1 -> new OffsetAndTimestamp(15, 0),
      tp2 -> new OffsetAndTimestamp(15, 0))
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, forTime)
    val consumer: TransientConsumer[IO]     = TransientConsumer[IO](topicName, pcs)
    val res = consumer.offsetRangeFor(NJDateTimeRange(sydneyTime).withToday).unsafeRunSync()
    assert(res.value.forall(_._2.forall(_.distance == 10)))
  }

}
