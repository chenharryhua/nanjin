package com.github.chenharryhua.nanjin.kafka.admins

import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.buildConsumer.*
import com.github.chenharryhua.nanjin.kafka.{buildConsumer, PureConsumerSettings, TopicName}
import fs2.kafka.admin.MkAdminClient
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.{AdminClientSettings, KafkaAdminClient}
import org.apache.kafka.clients.admin.{AdminClient, MockAdminClient, TopicDescription}
import org.apache.kafka.common.{Node, TopicPartition, TopicPartitionInfo}
import munit.CatsEffectSuite

import java.util
import scala.jdk.CollectionConverters.*

/** Offline coverage for `AdminTopic`.
  *
  * The admin-client operations (`newTopic`, `iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence`,
  * `mirrorTo`) run against a shared `MockAdminClient` (from the `kafka-clients` test jar) wired in through a
  * test-scoped `MkAdminClient`, so topic creation and deletion are observable without a broker. The
  * consumer-backed methods `partitionsFor` and `offsetRangeFor` delegate to a `SnapshotConsumer` built from
  * the in-memory `buildConsumer` stub.
  *
  * `retrieveRecord`, `acls`/`acls(principal)`, and `groups` are out of scope. `retrieveRecord` relies on a
  * live consumer: the `buildConsumer` stub's `poll` returns `null` rather than an empty record set.
  * `MockAdminClient` throws `UnsupportedOperationException` for `describeAcls`, and it cannot model per-group
  * committed offsets faithfully enough to exercise the `groups` topic-membership filter. Both need a live
  * broker, as `AdminApiTest` shows.
  */
class AdminTopicTest extends CatsEffectSuite {

  private val node: Node = new Node(0, "localhost", 9092)

  private def partitionInfo(partition: Int, replicas: Int): TopicPartitionInfo =
    new TopicPartitionInfo(
      partition,
      node,
      List.fill(replicas)(node).asJava,
      List.fill(replicas)(node).asJava)

  /** A `TopicDescription` with `partitions` partitions, each with `replicas` replicas. */
  private def topicDescription(name: String, partitions: Int, replicas: Int): TopicDescription = {
    val tpis: util.List[TopicPartitionInfo] =
      (0 until partitions).map(partitionInfo(_, replicas)).toList.asJava
    new TopicDescription(name, false, tpis)
  }

  /** Build an `AdminTopic` over a fresh shared `MockAdminClient` and the in-memory consumer. The same mock
    * instance backs the fs2-kafka `KafkaAdminClient`, so topics created or deleted through the wrapper are
    * observable by querying `adminClient` again.
    */
  private def adminTopic(
    begin: Map[TopicPartition, java.lang.Long] = Map.empty,
    end: Map[TopicPartition, java.lang.Long] = Map.empty): Resource[IO, AdminTopic[IO]] = {
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    implicit val mkAdmin: MkAdminClient[IO] = (_: AdminClientSettings) =>
      // at least one broker is required, otherwise createTopic cannot place replicas
      IO(new MockAdminClient(List(node).asJava, node): AdminClient)
    for {
      admin <- KafkaAdminClient.resource[IO](AdminClientSettings("localhost:9092"))
      consumer <- Resource.eval(mkConsumer(PureConsumerSettings).map(SnapshotConsumer[IO](topicName, _)))
    } yield AdminTopic[IO](admin, consumer, topicName)
  }

  test("1.newTopic(numPartitions, numReplicas) creates the topic with that shape") {
    adminTopic().use { admin =>
      for {
        _ <- admin.newTopic(3, 1)
        desc <- admin.adminClient.describeTopics(List(topicName.value))
      } yield {
        val td = desc(topicName.value)
        assertEquals(td.partitions().size(), 3)
        assertEquals(td.partitions().get(0).replicas().size(), 1)
      }
    }
  }

  test("2.newTopic(description) creates a topic matching the description's partition and replica counts") {
    adminTopic().use { admin =>
      for {
        _ <- admin.newTopic(topicDescription("ignored-source-name", partitions = 2, replicas = 1))
        desc <- admin.adminClient.describeTopics(List(topicName.value))
      } yield {
        // the description's name is irrelevant; the topic is created under this admin's topicName
        val td = desc(topicName.value)
        assertEquals(td.partitions().size(), 2)
        assertEquals(td.partitions().get(0).replicas().size(), 1)
      }
    }
  }

  test("3.delete removes a previously created topic") {
    adminTopic().use { admin =>
      for {
        _ <- admin.newTopic(1, 1)
        _ <- admin.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
        listed <- admin.adminClient.listTopics.names
      } yield assert(!listed.contains(topicName.value), s"topic still present: $listed")
    }
  }

  test("4.delete of a non-existent topic fails") {
    adminTopic().use { admin =>
      admin.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt.map { res =>
        assert(res.isLeft, "expected deleting an absent topic to fail")
      }
    }
  }

  test("5.mirrorTo creates a new topic with the source topic's partition and replica shape") {
    val mirror = "admin-topic-test.mirror"
    adminTopic().use { admin =>
      for {
        _ <- admin.newTopic(3, 1)
        _ <- admin.mirrorTo(TopicName(mirror))
        desc <- admin.adminClient.describeTopics(List(topicName.value, mirror))
      } yield {
        val src = desc(topicName.value)
        val dst = desc(mirror)
        assertEquals(dst.partitions().size(), src.partitions().size())
        assertEquals(dst.partitions().get(0).replicas().size(), src.partitions().get(0).replicas().size())
      }
    }
  }

  test("6.partitionsFor delegates to the consumer") {
    adminTopic().use(_.partitionsFor).map { res =>
      assertEquals(res.toSet, Set(tp0, tp1, tp2))
    }
  }

  test("7.offsetRangeFor delegates to the consumer") {
    val begin: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 0L, tp1 -> 2, tp2 -> 4)
    val end: Map[TopicPartition, java.lang.Long] = Map(tp0 -> 10L, tp1 -> 10, tp2 -> 10)
    adminTopic(begin, end).use(_.offsetRangeFor(DateTimeRange(sydneyTime))).map { res =>
      assertEquals(res.treeMap.size, 3)
      assert(res.get(tp0).flatten.map(_.distance).contains(10L))
      assert(res.get(tp1).flatten.map(_.distance).contains(8L))
      assert(res.get(tp2).flatten.map(_.distance).contains(6L))
    }
  }
}
