package com.github.chenharryhua.nanjin.kafka.connector

import cats.Id
import com.github.chenharryhua.nanjin.kafka.TopicName
import fs2.kafka.consumer.KafkaTopics
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.FiniteDuration

class TopicUtilsTest extends AnyFunSuite {

  private val topic: String = "topic-utils-test"

  /** A minimal in-memory `KafkaTopics[Id]`: `numPartitions` partitions, each spanning `[begin, end)`. Only
    * the three methods `get_offset_range_by_offsets` needs return meaningful values.
    */
  private def fakeTopics(numPartitions: Int, begin: Long, end: Long): KafkaTopics[Id] =
    new KafkaTopics[Id] {
      private val tps: List[TopicPartition] =
        (0 until numPartitions).map(new TopicPartition(topic, _)).toList

      override def partitionsFor(t: String): Id[List[PartitionInfo]] =
        tps.map(tp => new PartitionInfo(tp.topic(), tp.partition(), null, Array.empty, Array.empty))
      override def partitionsFor(t: String, timeout: FiniteDuration): Id[List[PartitionInfo]] =
        partitionsFor(t)

      override def beginningOffsets(ps: Set[TopicPartition]): Id[Map[TopicPartition, Long]] =
        ps.map(_ -> begin).toMap
      override def beginningOffsets(
        ps: Set[TopicPartition],
        timeout: FiniteDuration): Id[Map[TopicPartition, Long]] = beginningOffsets(ps)

      override def endOffsets(ps: Set[TopicPartition]): Id[Map[TopicPartition, Long]] =
        ps.map(_ -> end).toMap
      override def endOffsets(
        ps: Set[TopicPartition],
        timeout: FiniteDuration): Id[Map[TopicPartition, Long]] = endOffsets(ps)

      override def offsetsForTimes(
        ts: Map[TopicPartition, Long]): Id[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
        ts.map { case (tp, _) => tp -> None }
      override def offsetsForTimes(
        ts: Map[TopicPartition, Long],
        timeout: FiniteDuration): Id[Map[TopicPartition, Option[OffsetAndTimestamp]]] =
        offsetsForTimes(ts)

      override def listTopics: Id[Map[String, List[PartitionInfo]]] = Map.empty
      override def listTopics(timeout: FiniteDuration): Id[Map[String, List[PartitionInfo]]] = Map.empty
    }

  private def resolve(client: KafkaTopics[Id], pos: Map[Int, (Long, Long)]): Map[Int, (Long, Long)] =
    topicUtils
      .get_offset_range[Id](client, TopicName(topic), Right(pos))
      .toList
      .map { case (tp, or) => tp.partition() -> (or.from, or.until) }
      .toMap

  test("1.explicit range within the topic bounds is preserved") {
    val client = fakeTopics(numPartitions = 1, begin = 0L, end = 100L)
    val res = resolve(client, Map(0 -> (10L, 50L)))
    assert(res === Map(0 -> (10L, 50L)))
  }

  test("2.explicit range is clamped to the topic's [begin, end)") {
    val client = fakeTopics(numPartitions = 1, begin = 20L, end = 80L)
    // request beyond both ends -> clamped to [20, 80)
    val res = resolve(client, Map(0 -> (0L, 1000L)))
    assert(res === Map(0 -> (20L, 80L)))
  }

  test("3.partition not on the topic is dropped") {
    val client = fakeTopics(numPartitions = 1, begin = 0L, end = 100L)
    // partition 5 does not exist; only partition 0 survives
    val res = resolve(client, Map(0 -> (0L, 10L), 5 -> (0L, 10L)))
    assert(res.keySet === Set(0))
  }

  test("4.empty clamped range is dropped") {
    val client = fakeTopics(numPartitions = 1, begin = 50L, end = 100L)
    // requested range lies entirely before the topic start -> clamped to empty -> dropped
    val res = resolve(client, Map(0 -> (0L, 40L)))
    assert(res.isEmpty)
  }

  test("5.multiple partitions each clamped independently") {
    val client = fakeTopics(numPartitions = 2, begin = 0L, end = 100L)
    val res = resolve(client, Map(0 -> (10L, 20L), 1 -> (50L, 500L)))
    assert(res === Map(0 -> (10L, 20L), 1 -> (50L, 100L)))
  }
}
