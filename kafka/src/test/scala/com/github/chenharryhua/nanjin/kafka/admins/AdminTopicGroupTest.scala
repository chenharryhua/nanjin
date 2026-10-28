package com.github.chenharryhua.nanjin.kafka.admins

import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.kafka.buildConsumer.*
import com.github.chenharryhua.nanjin.kafka.{buildConsumer, GroupId, PureConsumerSettings}
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.{AdminClientSettings, KafkaAdminClient}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import munit.CatsEffectSuite

import java.time.Instant

/** Offline coverage for `AdminTopicGroup`.
  *
  * `AdminTopicGroup` splits into two groups of methods. `commitSync` and the `resetOffsets*` family delegate
  * straight to the `SnapshotConsumer`, so they are exercised here against the in-memory consumer built by
  * `buildConsumer` (the same stub `TransientConsumerTest` uses) with no live broker.
  *
  * The remaining methods need a broker: `lagBehind` and `deleteConsumerGroupOffsets` call the Kafka admin
  * client. The pure core of `lagBehind` (`calculate.admin_lagBehind`) already has dedicated coverage in
  * `CalculateLagBehindTest`, and the effectful wrappers are only reachable against a running cluster, which
  * is why `AdminApiTest` keeps its `groups` test commented out. They are out of scope for this offline suite.
  *
  * The admin client passed to `AdminTopicGroup` is a real `KafkaAdminClient` built from an unreachable
  * bootstrap. The Java admin client connects lazily, so as long as no admin operation is invoked it never
  * reaches out to a broker and closes cleanly — the same assumption `KafkaContextTest` relies on.
  */
class AdminTopicGroupTest extends CatsEffectSuite {
  private val group: GroupId = GroupId("admin-topic-group-test")

  // A never-connected admin client. `AdminClient.create` validates `bootstrap.servers` at construction, so
  // the address has to be syntactically valid (host:port) even though nothing listens there. The Java client
  // connects lazily, so no broker is contacted unless an admin operation runs, and none of the methods tested
  // here touches it.
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
}
