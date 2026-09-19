package com.github.chenharryhua.nanjin.kafka.admins

import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.common.chrono.zones.darwinTime
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.admins.SnapshotConsumer
import com.github.chenharryhua.nanjin.kafka.connector.ConsumeKafka
import com.github.chenharryhua.nanjin.kafka.serdes.Primitive
import com.github.chenharryhua.nanjin.kafka.{
  Offset,
  OffsetRange,
  PureConsumerSettings,
  TopicDef,
  TopicPartitionMap
}
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords, ProducerResult}
import mtest.kafka.ctx
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import munit.CatsEffectSuite

import java.time.Instant

class ConsumerApiOffsetRangeTest extends CatsEffectSuite {

  /*
   *
   * ---------------------100-------200-------300---------------> Time
   * ---before beginning--|                     |-after ending---
   *
   */

  val pi = Primitive[java.lang.Integer]
  val topic: TopicDef[java.lang.Integer, java.lang.Integer] =
    TopicDef("range.test", pi, pi)

  val pr1: ProducerRecord[java.lang.Integer, java.lang.Integer] =
    ProducerRecord(topic.topicName.value, Integer.valueOf(1), Integer.valueOf(1)).withTimestamp(100)
  val pr2: ProducerRecord[java.lang.Integer, java.lang.Integer] =
    ProducerRecord(topic.topicName.value, Integer.valueOf(2), Integer.valueOf(2)).withTimestamp(200)
  val pr3: ProducerRecord[java.lang.Integer, java.lang.Integer] =
    ProducerRecord(topic.topicName.value, Integer.valueOf(3), Integer.valueOf(3)).withTimestamp(300)

  val topicData: Stream[IO, ProducerResult[Integer, Integer]] =
    Stream(ProducerRecords(List(pr1, pr2, pr3)))
      .covary[IO]
      .unchunks
      .through(ctx.produce(topic).sink)

  // Suite-local fixture that runs the topic setup once before the tests. Replaces the class-level
  // `.unsafeRunSync()` block, which cannot run under CatsEffectSuite (no IORuntime in scope).
  private val setup: IO[Unit] =
    (ctx
      .admin(topic.topicName.value)
      .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
      topicData.compile.drain).void

  private val setupFixture =
    ResourceSuiteLocalFixture("consumer-api-offset-range-setup", Resource.eval(setup))

  override def munitFixtures = List(setupFixture)

  val transientConsumer: Resource[IO, SnapshotConsumer[IO]] =
    SnapshotConsumer[IO](
      topic.topicName,
      PureConsumerSettings
        .withProperties(ctx.settings.consumerSettings.properties)
        .withGroupId("consumer-api-test"))

  val client: ConsumeKafka[IO, Integer, Integer] = ctx.consume(topic)

  test("1.start and end are both in range") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(1), Offset(2))))

    val r = DateTimeRange(darwinTime).withStartTime(110).withEndTime(250)

    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      tpm <- client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError
    } yield assert(tpm == expect.flatten)
  }

  test("2.start > end") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(Map(new TopicPartition("range.test", 0) -> None))

    val r = DateTimeRange(darwinTime).withStartTime(250).withEndTime(110)

    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      res <- client.circumscribedStream(r).take(1).compile.last
    } yield assert(res.isEmpty)
  }

  test("3.when end is exactly match") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(Map(new TopicPartition("range.test", 0) -> OffsetRange(Offset(0), Offset(2))))

    val r = DateTimeRange(darwinTime).withStartTime(0).withEndTime(300)

    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      tpm <- client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError
    } yield assert(tpm == expect.flatten)
  }

  test("4.start is equal to beginning and end is equal to ending") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(0), Offset(2))))

    val r = DateTimeRange(darwinTime).withStartTime(100).withEndTime(300)

    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      tpm <- client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError
    } yield assert(tpm == expect.flatten)
  }

  test("5.start is equal to beginning and end is after ending") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(0), Offset(3))))

    val r = DateTimeRange(darwinTime).withStartTime(100).withEndTime(310)

    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      tpm <- client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError
    } yield assert(tpm == expect.flatten)
  }

  test("6.start after beginning and end after ending") {
    val expect =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(1), Offset(3))))

    val r = DateTimeRange(darwinTime).withStartTime(110).withEndTime(500)

    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      tpm <- client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError
    } yield assert(tpm == expect.flatten)
  }

  test("7.start before beginning and end before ending") {
    val expect =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(0), Offset(1))))

    val r = DateTimeRange(darwinTime).withStartTime(10).withEndTime(110)

    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      tpm <- client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError
    } yield assert(tpm == expect.flatten)
  }

  test("8.both start and end are before beginning") {
    val expect =
      TopicPartitionMap(Map(new TopicPartition("range.test", 0) -> None))

    val r = DateTimeRange(darwinTime).withStartTime(10).withEndTime(30)

    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      res <- client.circumscribedStream(r).take(1).compile.last
    } yield assert(res.isEmpty)
  }

  test("9.both start and end are after ending") {
    val expect =
      TopicPartitionMap(Map(new TopicPartition("range.test", 0) -> None))

    val r = DateTimeRange(darwinTime).withStartTime(500).withEndTime(600)

    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      res <- client.circumscribedStream(r).take(1).compile.last
    } yield assert(res.isEmpty)
  }

  test("10.when there is no data in the range") {
    val expect =
      TopicPartitionMap(Map(new TopicPartition("range.test", 0) -> None))

    val r = DateTimeRange(darwinTime).withStartTime(110).withEndTime(120)

    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      res <- client.circumscribedStream(r).take(1).compile.last
    } yield assert(res.isEmpty)
  }

  test("11.same range") {
    val r = DateTimeRange(darwinTime)
    val r2 = Map(0 -> (-1000L, 1000000000L), 100 -> (0L, 9999L))
    for {
      res <- client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError
      res2 <- client.circumscribedStream(r2).map(_.offsets).take(1).compile.lastOrError
    } yield assert(res == res2)
  }

  test("12.time range is infinite") {
    val expect: TopicPartitionMap[Option[OffsetRange]] =
      TopicPartitionMap(
        Map(new TopicPartition("range.test", 0) ->
          OffsetRange(Offset(0), Offset(3))))

    val r = DateTimeRange(darwinTime)
    for {
      _ <- transientConsumer.use(_.offsetRangeFor(r).map(x => assert(x == expect)))
      tpm <- client.circumscribedStream(r).map(_.offsets).take(1).compile.lastOrError
    } yield assert(tpm == expect.flatten)
  }

  test("13.kafka offset range") {
    assert(OffsetRange(Offset(100), Offset(99)).isEmpty)
    val r = OffsetRange(Offset(1), Offset(99)).get
    assert(r.distance == 98)
  }

  test("14.offsetRangeSince") {
    transientConsumer.use(_.offsetRangeSince(Instant.ofEpochMilli(100))).map { r =>
      val v = r.flatten
      assert(v.nonEmpty)
    }
  }

  test("15.partitionsFor") {
    transientConsumer.use(_.partitionsFor).map { r =>
      assert(r.value.nonEmpty)
    }
  }

  test("16.retrieveRecordsForTimes") {
    transientConsumer.use(_.retrieveRecordsForTimes(Instant.ofEpochMilli(100))).map { r =>
      assert(r.nonEmpty)
    }
  }

  test("17.commitSync") {
    transientConsumer
      .use(_.commitSync(Map(new TopicPartition("range.test", 0) -> new OffsetAndMetadata(0))))
  }
}
