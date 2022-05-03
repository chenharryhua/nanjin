package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.*
import eu.timepit.refined.auto.*
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class AdminApiTest extends AnyFunSuite {
  val topic: KafkaTopic[IO, Int, Int]  = ctx.topic[Int, Int]("admin")
  val mirror: KafkaTopic[IO, Int, Int] = ctx.topic[Int, Int]("admin.mirror")

  test("newTopic") {
    val admin: KafkaAdminApi[IO] = topic.admin
    val run = for {
      _ <- admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt
      _ <- IO.sleep(1.seconds)
      _ <- admin.newTopic(3, 1)
      _ <- IO.sleep(1.seconds)
      info <- admin.describe
    } yield println(info)
    run.unsafeRunSync()

  }
  test("mirrorTo") {
    val admin: KafkaAdminApi[IO]  = topic.admin
    val madmin: KafkaAdminApi[IO] = mirror.admin
    val run = for {
      _ <- madmin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt
      _ <- IO.sleep(1.seconds)
      _ <- admin.mirrorTo(mirror.topicName, 1)
      _ <- IO.sleep(1.seconds)
      info <- admin.describe
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
