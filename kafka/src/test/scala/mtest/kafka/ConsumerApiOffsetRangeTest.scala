package mtest.kafka

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.{darwinTime, NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.{
  KafkaOffset,
  KafkaOffsetRange,
  KafkaPartition,
  KafkaTopic,
  KafkaTopicPartition,
  TopicDef,
  TopicName
}
import fs2.kafka.ProducerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
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

    val r = NJDateTimeRange(darwinTime).withStartTime(110).withEndTime(250)

    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }

  test("start after beginning and end after ending - invalid end") {
    val expect =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(1), KafkaOffset(3))))

    val r = NJDateTimeRange(darwinTime).withStartTime(110).withEndTime(500)

    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }

  test("start before beginning and end before ending - invalid start") {
    val expect =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(0), KafkaOffset(1))))

    val r = NJDateTimeRange(darwinTime).withStartTime(10).withEndTime(110)

    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }

  test("both start and end are before beginning - invalid both") {
    val expect =
      KafkaTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

    val r = NJDateTimeRange(darwinTime).withStartTime(10).withEndTime(30)

    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }
  test("both start and end are after ending - invalid both") {
    val expect =
      KafkaTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

    val r = NJDateTimeRange(darwinTime).withStartTime(500).withEndTime(600)

    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }

  test("time range is infinite") {
    val expect: KafkaTopicPartition[Option[KafkaOffsetRange]] =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(0), KafkaOffset(3))))

    val r = NJDateTimeRange(darwinTime)
    topic.shortLiveConsumer.use(_.offsetRangeFor(r)).map(x => assert(x === expect)).unsafeRunSync()
  }
  test("kafka offset range") {
    assert(KafkaOffsetRange(KafkaOffset(100), KafkaOffset(99)).isEmpty)
    val r = KafkaOffsetRange(KafkaOffset(1), KafkaOffset(99)).get
    assert(r.distance == 98)
  }
  test("reset") {
    topic
      .shortLiveConsumer
      .use(sc => sc.resetOffsetsForTimes(NJTimestamp(100)) >> sc.resetOffsetsToBegin >> sc.resetOffsetsToEnd)
      .unsafeRunSync()
  }
  test("retrieveRecord") {
    val r  = topic.shortLiveConsumer.use(_.retrieveRecord(KafkaPartition(0), KafkaOffset(0))).unsafeRunSync().get
    val r2 = topic.record(0, 0).unsafeRunSync().get
    assert(r.partition() == r2.partition())
    assert(r.offset() == r2.offset())

  }
  test("numOfRecordsSince") {
    val r = topic.shortLiveConsumer.use(_.numOfRecordsSince(NJTimestamp(100))).unsafeRunSync()
    val v = r.flatten[KafkaOffsetRange]
    assert(v.nonEmpty)
  }
  test("partitionsFor") {
    val r = topic.shortLiveConsumer.use(_.partitionsFor).unsafeRunSync()
    assert(r.value.nonEmpty)
  }
  test("retrieveRecordsForTimes") {
    val r = topic.shortLiveConsumer.use(_.retrieveRecordsForTimes(NJTimestamp(100))).unsafeRunSync()
    assert(r.nonEmpty)
  }
  test("commitSync") {
    topic
      .shortLiveConsumer
      .use(_.commitSync(Map(new TopicPartition("range.test", 0) -> new OffsetAndMetadata(0))))
      .unsafeRunSync()
  }

}
