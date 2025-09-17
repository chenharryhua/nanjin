package com.github.chenharryhua.nanjin.kafka

import cats.Id
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.darwinTime
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.{DateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.connector.ConsumeKafka
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{ConsumerSettings, ProducerRecord, ProducerRecords, ProducerResult}
import mtest.kafka.ctx
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

class ConsumerApiOffsetRangeTest extends AnyFunSuite {

  /*
   *
   * ---------------------100-------200-------300---------------> Time
   * ---before beginning--|                     |-after ending---
   *
   */

  val topicDef: AvroTopic[Int, Int] = AvroTopic[Int, Int](TopicName("range.test"))
  val topic: AvroTopic[Int, Int] = topicDef

  val pr1: ProducerRecord[Int, Int] = ProducerRecord(topic.topicName.value, 1, 1).withTimestamp(100)
  val pr2: ProducerRecord[Int, Int] = ProducerRecord(topic.topicName.value, 2, 2).withTimestamp(200)
  val pr3: ProducerRecord[Int, Int] = ProducerRecord(topic.topicName.value, 3, 3).withTimestamp(300)

  val topicData: Stream[IO, ProducerResult[Int, Int]] =
    Stream(ProducerRecords(List(pr1, pr2, pr3))).covary[IO].through(ctx.produce(topicDef).sink)

  (ctx
    .admin(topic.topicName.name)
    .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
    topicData.compile.drain).unsafeRunSync()

  val transientConsumer: TransientConsumer[IO] = {
    val cs: ConsumerSettings[Id, Nothing, Nothing] = ConsumerSettings[Id, Nothing, Nothing](null, null)
    TransientConsumer[IO](
      topic.topicName,
      cs.withProperties(ctx.settings.consumerSettings.properties).withGroupId("consumer-api-test"))
  }

  val client: ConsumeKafka[IO, Int, Int] = ctx.consume(topic)

  test("start and end are both in range") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(1), Offset(2))))

    val r = DateTimeRange(darwinTime).withStartTime(110).withEndTime(250)

    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val tpm = client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError.unsafeRunSync()
    assert(tpm === expect.flatten)
  }

  test("start > end") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(Map(new TopicPartition("range.test", 0) -> None))

    val r = DateTimeRange(darwinTime).withStartTime(250).withEndTime(110)

    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val res = client.circumscribedStream(r).take(1).compile.last.unsafeRunSync()
    assert(res.isEmpty)
  }

  test("when end is exactly match") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(Map(new TopicPartition("range.test", 0) -> OffsetRange(Offset(0), Offset(2))))

    val r = DateTimeRange(darwinTime).withStartTime(0).withEndTime(300)

    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val tpm = client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError.unsafeRunSync()
    assert(tpm === expect.flatten)
  }

  test("start is equal to beginning and end is equal to ending") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(0), Offset(2))))

    val r = DateTimeRange(darwinTime).withStartTime(100).withEndTime(300)

    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val tpm = client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError.unsafeRunSync()
    assert(tpm === expect.flatten)
  }

  test("start is equal to beginning and end is after ending") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(0), Offset(3))))

    val r = DateTimeRange(darwinTime).withStartTime(100).withEndTime(310)

    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val tpm = client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError.unsafeRunSync()
    assert(tpm === expect.flatten)

  }

  test("start after beginning and end after ending") {
    val expect =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(1), Offset(3))))

    val r = DateTimeRange(darwinTime).withStartTime(110).withEndTime(500)

    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val tpm = client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError.unsafeRunSync()
    assert(tpm === expect.flatten)

  }

  test("start before beginning and end before ending") {
    val expect =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(0), Offset(1))))

    val r = DateTimeRange(darwinTime).withStartTime(10).withEndTime(110)

    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val tpm = client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError.unsafeRunSync()
    assert(tpm === expect.flatten)

  }

  test("both start and end are before beginning") {
    val expect =
      TopicPartitionMap(Map(new TopicPartition("range.test", 0) -> None))

    val r = DateTimeRange(darwinTime).withStartTime(10).withEndTime(30)

    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val res = client.circumscribedStream(r).take(1).compile.last.unsafeRunSync()
    assert(res.isEmpty)

  }

  test("both start and end are after ending") {
    val expect =
      TopicPartitionMap(Map(new TopicPartition("range.test", 0) -> None))

    val r = DateTimeRange(darwinTime).withStartTime(500).withEndTime(600)

    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val res = client.circumscribedStream(r).take(1).compile.last.unsafeRunSync()
    assert(res.isEmpty)
  }

  test("when there is no data in the range") {
    val expect =
      TopicPartitionMap(Map(new TopicPartition("range.test", 0) -> None))

    val r = DateTimeRange(darwinTime).withStartTime(110).withEndTime(120)

    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val res = client.circumscribedStream(r).take(1).compile.last.unsafeRunSync()
    assert(res.isEmpty)

  }

  test("same range") {
    val r = DateTimeRange(darwinTime)
    val res = client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError.unsafeRunSync()
    val r2 = Map(0 -> (-1000L, 1000000000L), 100 -> (0L, 9999L))
    val res2 = client.circumscribedStream(r2).map(_.offsets).take(1).compile.lastOrError.unsafeRunSync()
    assert(res == res2)
  }

  test("time range is infinite") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(0), Offset(3))))

    val r = DateTimeRange(darwinTime)
    transientConsumer.offsetRangeFor(r).map(x => assert(x === expect)).unsafeRunSync()
    val tpm = client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError.unsafeRunSync()
    assert(tpm === expect.flatten)

  }

  test("kafka offset range") {
    assert(OffsetRange(Offset(100), Offset(99)).isEmpty)
    val r = OffsetRange(Offset(1), Offset(99)).get
    assert(r.distance == 98)
  }

  test("numOfRecordsSince") {
    val r = transientConsumer.numOfRecordsSince(NJTimestamp(100)).unsafeRunSync()
    val v = r.flatten
    assert(v.nonEmpty)
  }

  test("partitionsFor") {
    val r = transientConsumer.partitionsFor.unsafeRunSync()
    assert(r.value.nonEmpty)
  }

  test("retrieveRecordsForTimes") {
    val r = transientConsumer.retrieveRecordsForTimes(NJTimestamp(100)).unsafeRunSync()
    assert(r.nonEmpty)
  }

  test("commitSync") {
    transientConsumer
      .commitSync(Map(new TopicPartition("range.test", 0) -> new OffsetAndMetadata(0)))
      .unsafeRunSync()
  }
}
