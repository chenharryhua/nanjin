package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.{KafkaConsumerGroupInfo, KafkaOffset, KafkaTopicPartition}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import eu.timepit.refined.auto.*

class AdminApiTest extends AnyFunSuite {
  val topic  = ctx.topic[Int, Int]("admin")
  val mirror = ctx.topic[Int, Int]("admin.mirror")

  test("newTopic") {
    val run = for {
      d <- topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt
      _ <- IO.sleep(1.seconds)
      n <- topic.admin.newTopic(3, 1)
      _ <- IO.sleep(1.seconds)
      info <- topic.admin.describe
    } yield println(info)
    run.unsafeRunSync()

  }
  test("mirrorTo") {
    val run = for {
      d <- mirror.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt
      _ <- IO.sleep(1.seconds)
      n <- topic.admin.mirrorTo(mirror.topicName, 1)
      _ <- IO.sleep(1.seconds)
      info <- topic.admin.describe
    } yield println(info)
    run.unsafeRunSync()
  }

  test("groups") {
    topic.admin.groups.unsafeRunSync()
  }
  test("KafkaConsumerGroupInfo") {
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
