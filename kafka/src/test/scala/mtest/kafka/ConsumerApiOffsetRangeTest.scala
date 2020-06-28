package mtest.kafka

import cats.effect.IO
import cats.implicits._
import cats.kernel.UpperBounded
import com.github.chenharryhua.nanjin.datetime.{melbourneTime, NJDateTimeRange}
import com.github.chenharryhua.nanjin.kafka.{
  KafkaOffset,
  KafkaOffsetRange,
  KafkaTopic,
  KafkaTopicPartition,
  TopicDef,
  TopicName
}
import fs2.kafka.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

class ConsumerApiOffsetRangeTest extends AnyFunSuite {

  val topic: KafkaTopic[IO, Int, Int] = TopicDef[Int, Int](TopicName("range.test")).in(ctx)

  val pr1: ProducerRecord[Int, Int] = topic.fs2PR(1, 1).withTimestamp(100)
  val pr2: ProducerRecord[Int, Int] = topic.fs2PR(2, 2).withTimestamp(200)
  val pr3: ProducerRecord[Int, Int] = topic.fs2PR(3, 3).withTimestamp(300)

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.send(pr1) >> topic.send(pr2) >> topic.send(pr3)).unsafeRunSync()

  test("start and end are both in range - both valid") {
    val expect: KafkaTopicPartition[Option[KafkaOffsetRange]] =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(1), KafkaOffset(2))))

    val r = NJDateTimeRange(melbourneTime).withStartTime(110).withEndTime(250)

    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }

  test("start after beginning and end after ending - invalid end") {
    val expect =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(1), KafkaOffset(3))))

    val r = NJDateTimeRange(melbourneTime).withStartTime(110).withEndTime(500)

    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }

  test("start before beginning and end before ending - invalid start") {
    val expect =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(0), KafkaOffset(1))))

    val r = NJDateTimeRange(melbourneTime).withStartTime(10).withEndTime(110)

    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }

  test("both start and end are before beginning - invalid both") {
    val expect =
      KafkaTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

    val r = NJDateTimeRange(melbourneTime).withStartTime(10).withEndTime(30)

    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }
  test("both start and end are after ending - invalid both") {
    val expect =
      KafkaTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

    val r = NJDateTimeRange(melbourneTime).withStartTime(500).withEndTime(600)

    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }

  test("time range is infinite") {
    val expect: KafkaTopicPartition[Option[KafkaOffsetRange]] =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(0), KafkaOffset(3))))

    val r = NJDateTimeRange(melbourneTime)
    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }

}
