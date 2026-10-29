package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.kafka.serdes.Primitive
import io.circe.syntax.EncoderOps
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

class AdminApiTest extends CatsEffectSuite {
  private val topicDef: TopicDef[Integer, Integer] =
    TopicDef("admin", Primitive[Integer], Primitive[Integer])
  private val topic = topicDef
  private val mirror = topicDef.withTopicName("admin.mirror")

  private def ensure_topic: IO[Unit] =
    ctx.admin(topic.topicName.value).use(_.newTopic(3, 1).attempt).void >> IO.sleep(500.millis)

  test("1.newTopic") {
    ctx.admin(topic.topicName.value).use { admin =>
      for {
        _ <- admin.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt
        _ <- IO.sleep(1.seconds)
        _ <- admin.newTopic(3, 1)
        _ <- IO.sleep(1.seconds)
      } yield assert(admin != null)
    }
  }

  test("2.mirrorTo") {
    val admin = ctx.admin(topic.topicName.value)
    val madmin = ctx.admin(mirror.topicName.value)
    for {
      _ <- madmin.use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt)
      _ <- IO.sleep(1.seconds)
      _ <- admin.use(_.mirrorTo(mirror.topicName))
      _ <- IO.sleep(1.seconds)
    } yield ()
  }

  test("3.KafkaOffset") {
    val end: TopicPartitionMap[Option[Offset]] = TopicPartitionMap[Option[Offset]](
      Map(
        new TopicPartition("t", 0) -> Some(Offset(100)),
        new TopicPartition("t", 1) -> Some(Offset(100)),
        new TopicPartition("t", 2) -> None)
    )
    assertEquals(end.asJson.as[TopicPartitionMap[Option[Offset]]].toOption, Some(end))
  }

  test("4.groups finds a committed group and deleteConsumerGroupOffsets removes its topic offsets") {
    val group_id = "admin-topic-group-offset-test"

    ensure_topic >> ctx.admin(topic.topicName.value).use(_.partitionsFor).flatMap { partitions =>
      ctx.admin(topic.topicName.value, group_id).use { group_admin =>
        val offsets = partitions.toSet.map(_ -> new OffsetAndMetadata(0L)).toMap
        for {
          _ <- group_admin.commitSync(offsets)
          _ <- IO.sleep(500.millis)
          groups <- ctx.admin(topic.topicName.value).use(_.groups)
          before <- group_admin.adminClient
            .listConsumerGroupOffsets(group_id)
            .partitionsToOffsetAndMetadata
          _ <- group_admin.deleteConsumerGroupOffsets
          after <- group_admin.adminClient
            .listConsumerGroupOffsets(group_id)
            .partitionsToOffsetAndMetadata
        } yield {
          assert(groups.contains(GroupId(group_id)), s"group was not listed: $groups")
          assertEquals(before.keySet.intersect(offsets.keySet), offsets.keySet)
          assert(after.keys.forall(_.topic() != topic.topicName.value), s"topic offsets remain: $after")
        }
      }
    }
  }

  test("5.retrieveRecord reads the record at a produced partition and offset") {
    ensure_topic >> (for {
      metadata <- ctx.produce(topicDef).produceOne(Integer.valueOf(99), Integer.valueOf(100))
      record <- ctx
        .admin(topic.topicName.value)
        .use(_.retrieveRecord(metadata.partition(), metadata.offset()))
    } yield {
      assert(record.nonEmpty, s"record not found at $metadata")
      assertEquals(record.map(_.topic()), Some(topic.topicName.value))
      assertEquals(record.map(_.partition()), Some(metadata.partition()))
      assertEquals(record.map(_.offset()), Some(metadata.offset()))
    })
  }

  test("6.acls".ignore) {
    ctx.admin(topic.topicName.value).use { admin =>
      for {
        _ <- admin.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt
        _ <- IO.sleep(1.seconds)
        _ <- admin.newTopic(1, 1)
        _ <- IO.sleep(1.seconds)
        all <- admin.acls
        principal <- admin.acls("User:alice")
      } yield {
        assert(all.forall(_.principal() != null))
        assert(principal.forall(_.principal() == "User:alice"))
      }
    }
  }
}
