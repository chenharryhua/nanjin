package mtest.kafka

import cats.implicits._
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka._
import fs2.kafka.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

class ConsumerApiOffsetRangeTest extends AnyFunSuite {

  val rangeTopic = TopicDef[Int, Int]("range.test").in(ctx)

  val ts1 = NJTimestamp(100)
  val ts2 = NJTimestamp(200)
  val ts3 = NJTimestamp(300)
  val pr1 = ProducerRecord("range.test", 1, 1).withTimestamp(ts1.milliseconds)
  val pr2 = ProducerRecord("range.test", 2, 2).withTimestamp(ts2.milliseconds)
  val pr3 = ProducerRecord("range.test", 3, 3).withTimestamp(ts3.milliseconds)

  (rangeTopic.admin.idefinitelyWantToDeleteTheTopic >>
    rangeTopic.send(pr1) >> rangeTopic.send(pr2) >> rangeTopic.send(pr3)).unsafeRunSync()

  test("start and end are both in range") {
    val expect =
      NJTopicPartition(
        Map(new TopicPartition("range.test", 0) -> Some(
          KafkaOffsetRange(KafkaOffset(1), KafkaOffset(2)))))

    val r = NJDateTimeRange(Some(NJTimestamp(110)), Some(NJTimestamp(250)))

    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }

  test("start after beginning and end after ending") {
    val expect =
      NJTopicPartition(
        Map(new TopicPartition("range.test", 0) -> Some(
          KafkaOffsetRange(KafkaOffset(1), KafkaOffset(3)))))

    val r = NJDateTimeRange(Some(NJTimestamp(110)), Some(NJTimestamp(500)))

    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }

  test("start before beginning and end before ending") {
    val expect =
      NJTopicPartition(
        Map(new TopicPartition("range.test", 0) -> Some(
          KafkaOffsetRange(KafkaOffset(0), KafkaOffset(1)))))

    val r = NJDateTimeRange(Some(NJTimestamp(10)), Some(NJTimestamp(110)))

    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }

  test("both start and end are before beginning") {
    val expect =
      NJTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

    val r = NJDateTimeRange(Some(NJTimestamp(10)), Some(NJTimestamp(30)))

    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }
  test("both start and end are after ending") {
    val expect =
      NJTopicPartition(Map(new TopicPartition("range.test", 0) -> None))

    val r = NJDateTimeRange(Some(NJTimestamp(500)), Some(NJTimestamp(600)))

    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }

  test("time range is infinite") {
    val expect =
      NJTopicPartition(
        Map(new TopicPartition("range.test", 0) -> Some(
          KafkaOffsetRange(KafkaOffset(0), KafkaOffset(3)))))

    val r = NJDateTimeRange.infinite
    rangeTopic.consumerResource
      .use(_.offsetRangeFor(r))
      .map(x => assert(x === expect))
      .unsafeRunSync()
  }
}
