package com.github.chenharryhua.nanjin.kafka.admins

import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.kafka.buildConsumer.*
import com.github.chenharryhua.nanjin.kafka.{buildConsumer, GroupId, LagBehind, Offset, PureConsumerSettings}
import fs2.kafka.admin.MkAdminClient
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.{AdminClientSettings, KafkaAdminClient}
import org.apache.kafka.clients.admin.{
  AdminClient,
  ListConsumerGroupOffsetsOptions,
  ListConsumerGroupOffsetsResult,
  ListConsumerGroupOffsetsSpec,
  MockAdminClient
}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import munit.CatsEffectSuite

import java.time.Instant
import java.util
import scala.jdk.CollectionConverters.*

/** Offline coverage for `AdminTopicGroup`.
  *
  * `AdminTopicGroup` splits into two groups of methods. `commitSync` and the `resetOffsets*` family delegate
  * straight to the `SnapshotConsumer`, so they are exercised against the in-memory consumer built by
  * `buildConsumer` (the same stub `TransientConsumerTest` uses) with no live broker.
  *
  * `lagBehind` also calls the Kafka admin client (`listConsumerGroupOffsets`). It is driven here by seeding a
  * `MockAdminClient` (from the `kafka-clients` test jar) with committed group offsets and wiring it in via a
  * test-scoped `MkAdminClient`, so the full wrapper — end offsets from the consumer, committed offsets from
  * the admin client, the topic filter, and `calculate.admin_lagBehind` — runs offline. The pure computation
  * also has dedicated coverage in `CalculateLagBehindTest`.
  *
  * `deleteConsumerGroupOffsets` needs a broker because `MockAdminClient` leaves that operation unimplemented;
  * it is covered by the broker-backed `AdminApiTest`.
  *
  * The commit/reset tests build a real but never-connected `KafkaAdminClient`: `AdminClient.create` validates
  * `bootstrap.servers` at construction, so the address is a syntactically valid host:port even though nothing
  * listens there. The Java client connects lazily, so no broker is contacted unless an admin operation runs.
  */
class AdminTopicGroupTest extends CatsEffectSuite {
  private val group: GroupId = GroupId("admin-topic-group-test")

  // A never-connected admin client. `AdminClient.create` validates `bootstrap.servers` at construction, so
  // the address has to be syntactically valid (host:port) even though nothing listens there. The Java client
  // connects lazily, so no broker is contacted unless an admin operation runs, and none of the methods tested
  // through this client touch it.
  private val adminClient: Resource[IO, KafkaAdminClient[IO]] =
    KafkaAdminClient.resource[IO](AdminClientSettings("localhost:9092"))

  private def adminTopicGroup(
    begin: Map[TopicPartition, java.lang.Long] = Map.empty,
    end: Map[TopicPartition, java.lang.Long] = Map.empty): Resource[IO, AdminTopicGroup[IO]] = {
    implicit val mkConsumer: MkConsumer[IO] =
      buildConsumer(begin, end, Map.empty)
    for {
      admin <- adminClient
      consumer <- Resource.eval(mkConsumer(PureConsumerSettings).map(SnapshotConsumer[IO](topicName, _)))
    } yield AdminTopicGroup[IO](admin, consumer, topicName, group)
  }

  /** An `AdminTopicGroup` whose admin client is a `MockAdminClient` seeded with `committed` consumer-group
    * offsets, and whose consumer reports `end` as each partition's end offset. Lets `lagBehind` resolve both
    * sides of the lag computation without a broker.
    */
  private def mockAdminTopicGroup(
    committed: Map[TopicPartition, Long],
    end: Map[TopicPartition, java.lang.Long]): Resource[IO, AdminTopicGroup[IO]] = {
    implicit val mkConsumer: MkConsumer[IO] =
      buildConsumer(Map.empty, end, Map.empty)
    implicit val mkAdmin: MkAdminClient[IO] = (_: AdminClientSettings) =>
      IO {
        val mock = new MockAdminClient() {
          // fs2-kafka's `partitionsToOffsetAndMetadata` requests all partitions, which arrives as a spec with
          // a null `topicPartitions`. MockAdminClient 8.4.0-ce calls `isEmpty` on it without a null check, so
          // normalise null to an empty collection (its "all partitions" sentinel) before delegating.
          override def listConsumerGroupOffsets(
            groupSpecs: util.Map[String, ListConsumerGroupOffsetsSpec],
            options: ListConsumerGroupOffsetsOptions): ListConsumerGroupOffsetsResult = {
            groupSpecs.values().forEach { spec =>
              if (spec.topicPartitions() == null) {
                val _ = spec.topicPartitions(util.Collections.emptyList[TopicPartition]())
              }
            }
            super.listConsumerGroupOffsets(groupSpecs, options)
          }
        }
        mock.updateConsumerGroupOffsets(committed.map { case (k, v) =>
          k -> java.lang.Long.valueOf(v)
        }.asJava)
        mock: AdminClient
      }
    for {
      admin <- KafkaAdminClient.resource[IO](AdminClientSettings("localhost:9092"))
      consumer <- Resource.eval(mkConsumer(PureConsumerSettings).map(SnapshotConsumer[IO](topicName, _)))
    } yield AdminTopicGroup[IO](admin, consumer, topicName, group)
  }

  test("1.adminClient exposes the client the group was built with") {
    adminTopicGroup().use(atg => IO(assert(atg.adminClient != null)))
  }

  test("2.commitSync(partition, offset) commits a single offset through the consumer") {
    adminTopicGroup().use(_.commitSync(0, 42L))
  }

  test("3.commitSync(offsets) forwards the offset map to the consumer") {
    val offsets = Map(tp0 -> new OffsetAndMetadata(1L), tp1 -> new OffsetAndMetadata(2L))
    adminTopicGroup().use(_.commitSync(offsets))
  }

  test("4.commitSync(offsets) tolerates an empty map") {
    adminTopicGroup().use(_.commitSync(Map.empty))
  }

  test("5.resetOffsetsToBegin commits each partition's beginning offset") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    adminTopicGroup(begin, end).use(_.resetOffsetsToBegin)
  }

  test("6.resetOffsetsToEnd commits each partition's end offset") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    adminTopicGroup(begin, end).use(_.resetOffsetsToEnd)
  }

  test("7.resetOffsetsForTimes commits the offsets-for-times result") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    adminTopicGroup(begin, end).use(_.resetOffsetsForTimes(Instant.ofEpochSecond(1)))
  }

  test("8.the reset family runs in sequence against the same group") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 0, tp2 -> 0)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    adminTopicGroup(begin, end).use { atg =>
      atg.resetOffsetsToBegin >> atg.resetOffsetsToEnd >> atg.resetOffsetsForTimes(Instant.ofEpochSecond(1))
    }
  }

  test("9.lagBehind is end minus committed, per partition") {
    val committed = Map(tp0 -> 70L, tp1 -> 40L, tp2 -> 10L)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 100L, tp1 -> 50L, tp2 -> 10L)
    mockAdminTopicGroup(committed, end).use(_.lagBehind).map { res =>
      assertEquals(res.get(tp0).flatten, Some(LagBehind(Offset(70L), Offset(100L))))
      assertEquals(res.get(tp0).flatten.map(_.lag), Some(30L))
      assertEquals(res.get(tp1).flatten, Some(LagBehind(Offset(40L), Offset(50L))))
      assertEquals(res.get(tp1).flatten.map(_.lag), Some(10L))
      // caught up: zero lag
      assertEquals(res.get(tp2).flatten, Some(LagBehind(Offset(10L), Offset(10L))))
      assertEquals(res.get(tp2).flatten.map(_.lag), Some(0L))
    }
  }

  test("10.lagBehind drops committed offsets that belong to other topics") {
    val other = new TopicPartition("some.other.topic", 0)
    val committed = Map(tp0 -> 70L, other -> 5L)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 100L)
    mockAdminTopicGroup(committed, end).use(_.lagBehind).map { res =>
      assertEquals(res.get(tp0).flatten, Some(LagBehind(Offset(70L), Offset(100L))))
      // the other topic's partition never appears in the result
      assert(res.get(other).isEmpty, s"other-topic partition leaked into result: $res")
    }
  }

  test("11.lagBehind yields None for a partition with an end offset but no committed offset") {
    // tp0 is committed; tp1 has an end offset but the group never committed to it -> leftCombine keeps the
    // key with None
    val committed = Map(tp0 -> 70L)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 100L, tp1 -> 50L)
    mockAdminTopicGroup(committed, end).use(_.lagBehind).map { res =>
      assertEquals(res.get(tp0).flatten, Some(LagBehind(Offset(70L), Offset(100L))))
      assertEquals(res.get(tp1), Some(None))
    }
  }
}
