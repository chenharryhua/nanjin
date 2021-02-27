package mtest.kafka

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.datetime.{darwinTime, NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka._
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords, ProducerResult}
import org.apache.kafka.common.TopicPartition
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class ConsumerApiOffsetRangeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  /** * Notes:
    * -----------offset(0)--offset(1)--offset(2)
    * ---------------100-------200-------300-------> Time(millisecond)
    * ----------------|                     |------
    * before beginning                       after ending
    *
    *                      ^             ^
    *                      |             |
    *                    start          end
    */

  val topic: KafkaTopic[IO, Int, Int] = ctx.withGroupId("consumer-api-test").topic[Int, Int]("range.test")

  val pr1: ProducerRecord[Int, Int] = ProducerRecord(topic.topicName.value, 1, 1).withTimestamp(100)
  val pr2: ProducerRecord[Int, Int] = ProducerRecord(topic.topicName.value, 2, 2).withTimestamp(200)
  val pr3: ProducerRecord[Int, Int] = ProducerRecord(topic.topicName.value, 3, 3).withTimestamp(300)

  val topicData: Stream[IO, ProducerResult[Unit, Int, Int]] =
    Stream(ProducerRecords(List(pr1, pr2, pr3))).covary[IO].through(topic.fs2Channel.producerPipe)

  "Offset Range" - {
    "data ingestion" in {
      (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
        topicData.compile.drain).assertNoException
    }

    "start and end are both in range" in {
      val expect: KafkaTopicPartition[Option[KafkaOffsetRange]] =
        KafkaTopicPartition(
          Map(new TopicPartition("range.test", 0) ->
            KafkaOffsetRange(KafkaOffset(1), KafkaOffset(2))))

      val r = NJDateTimeRange(darwinTime).withStartTime(110).withEndTime(250)

      topic.shortLiveConsumer.use(_.offsetRangeFor(r)).asserting(_ shouldBe expect)
    }

    "start is equal to beginning and end is equal to ending" in {
      val expect: KafkaTopicPartition[Option[KafkaOffsetRange]] =
        KafkaTopicPartition(
          Map(new TopicPartition("range.test", 0) ->
            KafkaOffsetRange(KafkaOffset(0), KafkaOffset(2))))

      val r = NJDateTimeRange(darwinTime).withStartTime(100).withEndTime(300)

      topic.shortLiveConsumer.use(_.offsetRangeFor(r)).asserting(_ shouldBe expect)
    }

    "start is equal to beginning and end is after ending" in {
      val expect: KafkaTopicPartition[Option[KafkaOffsetRange]] =
        KafkaTopicPartition(
          Map(new TopicPartition("range.test", 0) ->
            KafkaOffsetRange(KafkaOffset(0), KafkaOffset(3))))

      val r = NJDateTimeRange(darwinTime).withStartTime(100).withEndTime(310)

      topic.shortLiveConsumer.use(_.offsetRangeFor(r)).asserting(_ shouldBe expect)
    }

    "start after beginning and end after ending" in {
      val expect =
        KafkaTopicPartition(
          Map(new TopicPartition("range.test", 0) ->
            KafkaOffsetRange(KafkaOffset(1), KafkaOffset(3))))

      val r = NJDateTimeRange(darwinTime).withStartTime(110).withEndTime(500)

      topic.shortLiveConsumer.use(_.offsetRangeFor(r)).asserting(_ shouldBe expect)
    }

    "start before beginning and end before ending" in {
      val expect =
        KafkaTopicPartition(
          Map(new TopicPartition("range.test", 0) ->
            KafkaOffsetRange(KafkaOffset(0), KafkaOffset(1))))

      val r = NJDateTimeRange(darwinTime).withStartTime(10).withEndTime(110)

      topic.shortLiveConsumer.use(_.offsetRangeFor(r)).asserting(_ shouldBe expect)
    }

    "both start and end are before beginning" in {
      val expect =
        KafkaTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

      val r = NJDateTimeRange(darwinTime).withStartTime(10).withEndTime(30)

      topic.shortLiveConsumer.use(_.offsetRangeFor(r)).asserting(_ shouldBe expect)
    }

    "both start and end are after ending" in {
      val expect =
        KafkaTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

      val r = NJDateTimeRange(darwinTime).withStartTime(500).withEndTime(600)

      topic.shortLiveConsumer.use(_.offsetRangeFor(r)).asserting(_ shouldBe expect)
    }

    "when there is no data in the range" in {
      val expect =
        KafkaTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

      val r = NJDateTimeRange(darwinTime).withStartTime(110).withEndTime(120)

      topic.shortLiveConsumer.use(_.offsetRangeFor(r)).asserting(_ shouldBe expect)
    }

    "infinite time range" in {
      val expect: KafkaTopicPartition[Option[KafkaOffsetRange]] =
        KafkaTopicPartition(
          Map(new TopicPartition("range.test", 0) ->
            KafkaOffsetRange(KafkaOffset(0), KafkaOffset(3))))

      val r = NJDateTimeRange(darwinTime)
      topic.shortLiveConsumer.use(_.offsetRangeFor(r)).asserting(_ shouldBe expect)
    }

    "kafka offset range" in {
      assert(KafkaOffsetRange(KafkaOffset(100), KafkaOffset(99)).isEmpty)
      val r = KafkaOffsetRange(KafkaOffset(1), KafkaOffset(99)).get
      assert(r.distance == 98)
    }

    "reset to begin" in {
      val run = topic.shortLiveConsumer.use(sc => sc.resetOffsetsToBegin) >> topic.admin.groups
      run.asserting(_.head.lag.flatten.get(topic.topicName.value, 0).get.distance shouldBe 3)
    }

    "reset to end" in {
      val run = topic.shortLiveConsumer.use(sc => sc.resetOffsetsToEnd) >> topic.admin.groups
      run.asserting(_.head.lag.flatten.get(topic.topicName.value, 0) shouldBe None)
    }

    "reset to timestamp" in {
      val run = topic.shortLiveConsumer.use(sc => sc.resetOffsetsForTimes(NJTimestamp(150))) >> topic.admin.groups
      run.asserting(_.head.lag.flatten.get(topic.topicName.value, 0).get.from.offset.value shouldBe 1)
    }

    "retrieveRecord" in {
      for {
        r <- topic.shortLiveConsumer.use(_.retrieveRecord(KafkaPartition(0), KafkaOffset(0)))
        r2 <- topic.record(0, 0)
      } yield {
        assert(r.get.partition() == r2.get.partition())
        assert(r.get.offset() == r2.get.offset())
      }
    }

    "numOfRecordsSince" in {
      val r = topic.shortLiveConsumer.use(_.numOfRecordsSince(NJTimestamp(100))).map(_.flatten)
      r.asserting(_.nonEmpty shouldBe true)
    }

    "partitionsFor" in {
      val r = topic.shortLiveConsumer.use(_.partitionsFor)
      r.asserting(_.value.nonEmpty shouldBe true)
    }

    "retrieveRecordsForTimes" in {
      val r = topic.shortLiveConsumer.use(_.retrieveRecordsForTimes(NJTimestamp(100)))
      r.asserting(_.nonEmpty shouldBe true)
    }
  }
}
