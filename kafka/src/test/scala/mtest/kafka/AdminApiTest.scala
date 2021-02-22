package mtest.kafka

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.kafka.{KafkaConsumerGroupInfo, KafkaOffset, KafkaTopic, KafkaTopicPartition}
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class AdminApiTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  val topic: KafkaTopic[IO, Int, Int]  = ctx.topic[Int, Int]("admin")
  val mirror: KafkaTopic[IO, Int, Int] = ctx.topic[Int, Int]("admin.mirror")

  "admin" - {
    "newTopic" in {
      val run: IO[Map[String, TopicDescription]] = for {
        d <- topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
        _ <- IO.sleep(1.seconds)
        n <- topic.admin.newTopic(3, 1)
        _ <- IO.sleep(1.seconds)
        info <- topic.admin.describe
      } yield info
      run.asserting(_("admin").partitions().size() shouldBe 3)
    }

    "mirrorTo" in {
      val run = for {
        d <- mirror.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
        _ <- IO.sleep(1.seconds)
        n <- topic.admin.mirrorTo(mirror.topicName, 1)
        _ <- IO.sleep(1.seconds)
        info <- mirror.admin.describe
      } yield info
      run.asserting(_("admin.mirror").partitions().size() shouldBe 3)
    }

    "groups" in {
      topic.admin.groups.asserting(_.size shouldBe 0)
    }

    "KafkaConsumerGroupInfo" in {
      val end: KafkaTopicPartition[Option[KafkaOffset]] = KafkaTopicPartition[Option[KafkaOffset]](
        Map(
          new TopicPartition("t", 0) -> Some(KafkaOffset(100)),
          new TopicPartition("t", 1) -> Some(KafkaOffset(100)),
          new TopicPartition("t", 2) -> None)
      )
      val offsetMeta: Map[TopicPartition, OffsetAndMetadata] = Map(
        new TopicPartition("t", 0) -> new OffsetAndMetadata(0),
        new TopicPartition("t", 1) -> new OffsetAndMetadata(10),
        new TopicPartition("t", 2) -> new OffsetAndMetadata(20)
      )
      val cgi = KafkaConsumerGroupInfo("gid", end, offsetMeta)
      assert(cgi.lag.value.values.toList.flatten.map(_.distance).toSet == Set(100, 90))
    }
  }
}
