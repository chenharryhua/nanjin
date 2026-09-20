package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.kafka.serdes.Primitive
import io.circe.syntax.EncoderOps
import org.apache.kafka.common.TopicPartition
import munit.CatsEffectSuite

import scala.concurrent.duration.DurationInt

class AdminApiTest extends CatsEffectSuite {
  private val topicDef: TopicDef[Integer, Integer] =
    TopicDef("admin", Primitive[Integer], Primitive[Integer])
  private val topic = topicDef
  private val mirror = topicDef.withTopicName("admin.mirror")

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

//  test("groups") {
//    val tpo = Map(new TopicPartition(topic.topicName.value, 0) -> new OffsetAndMetadata(0))
//    val gp =
//      ctx.produce(topicDef).produceOne(0, 0) >> ctx.admin(TopicName("admin"), "groupid").use { admin =>
//        ctx.admin.use(_.listTopics.listings) >>
//          admin.commitSync(tpo) >>
//          admin.resetOffsetsToBegin >>
//          admin.resetOffsetsForTimes(NJTimestamp(0)) >>
//          admin.resetOffsetsToEnd >>
//          admin.lagBehind
//      }
//    gp.unsafeRunSync()
//  }

  test("3.KafkaOffset") {
    val end: TopicPartitionMap[Option[Offset]] = TopicPartitionMap[Option[Offset]](
      Map(
        new TopicPartition("t", 0) -> Some(Offset(100)),
        new TopicPartition("t", 1) -> Some(Offset(100)),
        new TopicPartition("t", 2) -> None)
    )
    assert(end.asJson.as[TopicPartitionMap[Option[Offset]]].toOption.get == end)
  }

  test("4.acls".ignore) {
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
