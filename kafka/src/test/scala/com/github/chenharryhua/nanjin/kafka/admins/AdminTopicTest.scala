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
import org.apache.kafka.clients.admin.{
  AdminClient,
  DescribeAclsOptions,
  DescribeAclsResult,
  MockAdminClient,
  TopicDescription
}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.acl.{
  AccessControlEntry,
  AccessControlEntryFilter,
  AclBinding,
  AclBindingFilter,
  AclOperation,
  AclPermissionType
}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.{Node, TopicPartition, TopicPartitionInfo}
import munit.CatsEffectSuite

import java.util
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

/** Offline coverage for `AdminTopic`.
  *
  * The admin-client operations (`newTopic`, `iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence`,
  * `mirrorTo`) run against a shared `MockAdminClient` (from the `kafka-clients` test jar) wired in through a
  * test-scoped `MkAdminClient`, so topic creation and deletion are observable without a broker. The
  * consumer-backed methods `partitionsFor` and `offsetRangeFor` delegate to a `SnapshotConsumer` built from
  * the in-memory `buildConsumer` stub.
  *
  * `retrieveRecord` and `groups` need a live consumer/group and are covered by the broker-backed
  * `AdminApiTest`. The stock `MockAdminClient` leaves `describeAcls` unimplemented, so the ACL tests below
  * override that public method to return a completed result and assert the exact filters built by
  * `AdminTopic`.
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
    end: Map[TopicPartition, java.lang.Long] = Map.empty,
    client: Option[AdminClient] = None): Resource[IO, AdminTopic[IO]] = {
    implicit val mkConsumer: MkConsumer[IO] = buildConsumer(begin, end, Map.empty)
    implicit val mkAdmin: MkAdminClient[IO] = (_: AdminClientSettings) =>
      // at least one broker is required, otherwise createTopic cannot place replicas
      IO(client.getOrElse(new MockAdminClient(List(node).asJava, node)))
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

  test("8.acls requests literal ACL bindings for this topic and returns their entries") {
    val captured = new AtomicReference[AclBindingFilter]()
    val entry = new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW)
    val binding =
      new AclBinding(new ResourcePattern(ResourceType.TOPIC, topicName.value, PatternType.LITERAL), entry)
    val mock = new MockAdminClient(List(node).asJava, node) {
      override def describeAcls(
        filter: AclBindingFilter,
        options: DescribeAclsOptions): DescribeAclsResult = {
        captured.set(filter)
        new DescribeAclsResult(
          KafkaFuture.completedFuture[java.util.Collection[AclBinding]](List(binding).asJava))
      }
    }

    adminTopic(client = Some(mock)).use(_.acls).map { result =>
      val filter = captured.get()
      assertEquals(result, List(entry))
      assertEquals(filter.patternFilter().resourceType(), ResourceType.TOPIC)
      assertEquals(filter.patternFilter().name(), topicName.value)
      assertEquals(filter.patternFilter().patternType(), PatternType.LITERAL)
      assertEquals(filter.entryFilter(), AccessControlEntryFilter.ANY)
    }
  }

  test("9.acls(principal) scopes the entry filter to the requested principal") {
    val captured = new AtomicReference[AclBindingFilter]()
    val entry = new AccessControlEntry("User:alice", "host", AclOperation.WRITE, AclPermissionType.ALLOW)
    val binding =
      new AclBinding(new ResourcePattern(ResourceType.TOPIC, topicName.value, PatternType.LITERAL), entry)
    val mock = new MockAdminClient(List(node).asJava, node) {
      override def describeAcls(
        filter: AclBindingFilter,
        options: DescribeAclsOptions): DescribeAclsResult = {
        captured.set(filter)
        new DescribeAclsResult(
          KafkaFuture.completedFuture[java.util.Collection[AclBinding]](List(binding).asJava))
      }
    }

    adminTopic(client = Some(mock)).use(_.acls("User:alice")).map { result =>
      val filter = captured.get().entryFilter()
      assertEquals(result, List(entry))
      assertEquals(filter.principal(), "User:alice")
      assertEquals(filter.host(), null)
      assertEquals(filter.operation(), AclOperation.ANY)
      assertEquals(filter.permissionType(), AclPermissionType.ANY)
    }
  }
}
