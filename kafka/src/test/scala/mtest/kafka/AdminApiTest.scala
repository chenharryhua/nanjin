package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.*
import eu.timepit.refined.auto.*
import io.circe.syntax.EncoderOps
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class AdminApiTest extends AnyFunSuite {
  val topic: KafkaTopic[IO, Int, Int]  = ctx.topic[Int, Int]("admin")
  val mirror: KafkaTopic[IO, Int, Int] = ctx.topic[Int, Int]("admin.mirror")

  test("newTopic") {
    val admin: KafkaAdminApi[IO] = ctx.admin(topic.topicName)
    val run = for {
      _ <- admin.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt
      _ <- IO.sleep(1.seconds)
      _ <- admin.newTopic(3, 1)
      _ <- IO.sleep(1.seconds)
      info <- admin.describe
    } yield println(info)
    run.unsafeRunSync()

  }
  test("mirrorTo") {
    val admin: KafkaAdminApi[IO]  = ctx.admin(topic.topicName)
    val madmin: KafkaAdminApi[IO] = ctx.admin(mirror.topicName)
    val run = for {
      _ <- madmin.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt
      _ <- IO.sleep(1.seconds)
      _ <- admin.mirrorTo(mirror.topicName, 1)
      _ <- IO.sleep(1.seconds)
      info <- admin.describe
    } yield println(info)
    run.unsafeRunSync()
  }

  test("groups") {
    val gp: List[KafkaConsumerGroupInfo] = ctx.admin(topic.topicName).groups.unsafeRunSync()
    assert(gp.asJson.as[List[KafkaConsumerGroupInfo]].toOption.get == gp)
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
    assert(end.asJson.as[KafkaTopicPartition[Option[KafkaOffset]]].toOption.get == end)

  }
}
