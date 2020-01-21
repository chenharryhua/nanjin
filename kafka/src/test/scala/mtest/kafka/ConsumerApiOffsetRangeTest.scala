package mtest.kafka

import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.{
  KafkaOffset,
  KafkaOffsetRange,
  KafkaTopicPartition,
  TopicDef
}
import fs2.kafka.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite
import cats.kernel.UpperBounded

class ConsumerApiOffsetRangeTest extends AnyFunSuite {

  val rangeTopic = TopicDef[Int, Int]("range.test").in(ctx)

  val pr1 = ProducerRecord("range.test", 1, 1).withTimestamp(100)
  val pr2 = ProducerRecord("range.test", 2, 2).withTimestamp(200)
  val pr3 = ProducerRecord("range.test", 3, 3).withTimestamp(300)

  (rangeTopic.admin.idefinitelyWantToDeleteTheTopic >>
    rangeTopic.send(pr1) >> rangeTopic.send(pr2) >> rangeTopic.send(pr3)).unsafeRunSync()

  test("start and end are both in range - both valid") {
    val expect: KafkaTopicPartition[Option[KafkaOffsetRange]] =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(1), KafkaOffset(2))))

    val r = NJDateTimeRange.infinite.withStartTime(110).withEndTime(250)

    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }

  test("start after beginning and end after ending - invalid end") {
    val expect =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(1), KafkaOffset(3))))

    val r = NJDateTimeRange.infinite.withStartTime(110).withEndTime(500)

    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }

  test("start before beginning and end before ending - invalid start") {
    val expect =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(0), KafkaOffset(1))))

    val r = NJDateTimeRange.infinite.withStartTime(10).withEndTime(110)

    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }

  test("both start and end are before beginning - invalid both") {
    val expect =
      KafkaTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

    val r = NJDateTimeRange.infinite.withStartTime(10).withEndTime(30)

    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }
  test("both start and end are after ending - invalid both") {
    val expect =
      KafkaTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

    val r = NJDateTimeRange.infinite.withStartTime(500).withEndTime(600)

    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }

  test("time range is infinite") {
    val expect: KafkaTopicPartition[Option[KafkaOffsetRange]] =
      KafkaTopicPartition(
        Map(new TopicPartition("range.test", 0) ->
          KafkaOffsetRange(KafkaOffset(0), KafkaOffset(3))))

    val r = UpperBounded[NJDateTimeRange].maxBound
    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }

}
