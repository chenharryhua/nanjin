package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.{DateTimeRange, NJTimestamp}
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
    val run = ctx.admin(topic.topicName).use { admin =>
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
    val admin  = ctx.admin(topic.topicName)
    val madmin = ctx.admin(mirror.topicName)
    val run = for {
      _ <- madmin.use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt)
      _ <- IO.sleep(1.seconds)
      _ <- admin.use(_.mirrorTo(mirror.topicName, 1))
      _ <- IO.sleep(1.seconds)
      info <- admin.use(_.describe)
    } yield println(info)
    run.unsafeRunSync()
  }

  test("groups") {
    val gid   = "g1"
    val tpo   = Map(new TopicPartition(topic.topicName.value, 0) -> new OffsetAndMetadata(0))
    val admin = ctx.admin("admin")
    val gp =
      ctx.producer[Int, Int].produceOne(topic.topicName.value, 0, 0) >> ctx.admin("admin").use { admin =>
        ctx.admin.use(_.listTopics.listings) >>
          admin.commitSync(gid, tpo) >>
          admin.retrieveRecord(0, 0) >>
          admin.resetOffsetsToBegin(gid) >>
          admin.resetOffsetsForTimes(gid, NJTimestamp(0)) >>
          admin.resetOffsetsToEnd(gid) >>
          admin.lagBehind(gid) >>
          admin.offsetRangeFor(DateTimeRange(sydneyTime).withToday) >>
          admin.partitionsFor >>
          admin.groups
      }
    assert(gp.unsafeRunSync().map(_.value).contains(gid))
    val gp2 = admin.use(am => am.deleteConsumerGroupOffsets(gid) >> am.groups)
    assert(!gp2.unsafeRunSync().map(_.value).contains(gid))
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
