package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.kafka.*
import eu.timepit.refined.auto.*
import io.circe.syntax.EncoderOps
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class AdminApiTest extends AnyFunSuite {
  private val topicDef: AvroTopic[Int, Int] = AvroTopic[Int, Int](TopicName("admin"))
  private val topic = topicDef
  private val mirror = topicDef.withTopicName("admin.mirror")

  test("newTopic") {
    val run = ctx.admin(topic.topicName.name).use { admin =>
      for {
        _ <- admin.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt
        _ <- IO.sleep(1.seconds)
        _ <- admin.newTopic(3, 1)
        _ <- IO.sleep(1.seconds)
        info <- admin.describe
      } yield println(info)
    }
    run.unsafeRunSync()
  }

  test("mirrorTo") {
    val admin = ctx.admin(topic.topicName.name)
    val madmin = ctx.admin(mirror.topicName.name)
    val run = for {
      _ <- madmin.use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt)
      _ <- IO.sleep(1.seconds)
      _ <- admin.use(_.mirrorTo(mirror.topicName))
      _ <- IO.sleep(1.seconds)
      info <- admin.use(_.describe)
    } yield println(info)
    run.unsafeRunSync()
  }

  test("groups") {
    val tpo = Map(new TopicPartition(topic.topicName.name.value, 0) -> new OffsetAndMetadata(0))
    val gp =
      ctx.produce(topicDef).produceOne(0, 0) >> ctx.admin("admin", "groupid").use { admin =>
        ctx.admin.use(_.listTopics.listings) >>
          admin.commitSync(tpo) >>
          admin.resetOffsetsToBegin >>
          admin.resetOffsetsForTimes(NJTimestamp(0)) >>
          admin.resetOffsetsToEnd >>
          admin.lagBehind
      }
    gp.unsafeRunSync()
  }

  test("KafkaOffset") {
    val end: TopicPartitionMap[Option[Offset]] = TopicPartitionMap[Option[Offset]](
      Map(
        new TopicPartition("t", 0) -> Some(Offset(100)),
        new TopicPartition("t", 1) -> Some(Offset(100)),
        new TopicPartition("t", 2) -> None)
    )
    assert(end.asJson.as[TopicPartitionMap[Option[Offset]]].toOption.get == end)
  }
}
