package mtest.kafka

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJDateTimeRange}
import com.github.chenharryhua.nanjin.kafka.{akkaSinks, KafkaOffset, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerResult}
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import scala.concurrent.duration.DurationInt

class AkkaChannelTest extends AnyFunSuite {
  val topic = ctx.topic[Int, String]("akka.consumer.test")

  val data: List[ProducerRecord[Int, String]] = List(
    topic.fs2PR(1, "a"),
    topic.fs2PR(2, "b"),
    topic.fs2PR(3, "c"),
    topic.fs2PR(4, "d"),
    topic.fs2PR(5, "e"))

  val sender: Stream[IO, List[ProducerResult[Int, String, Unit]]] =
    Stream.awakeEvery[IO](1.second).zipRight(Stream.eval(data.traverse(x => topic.send(x))))

  test("time-ranged") {
    val akkaChannel = topic.akkaChannel
    val range       = NJDateTimeRange(sydneyTime)
    (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >> IO.sleep(1.seconds))
      .unsafeRunSync()
    val res = akkaChannel
      .timeRanged(range)
      .delayBy(2.seconds)
      .map(x => topic.decoder(x).decode)
      .concurrently(sender)
      .interruptAfter(18.seconds)
      .compile
      .toList
      .unsafeRunSync()
      .map(x => ProducerRecord(x.topic, x.key(), x.value()))
    assert(res == data)
  }

  val vessel: TopicDef[PKey, aisClassAPositionReport] =
    TopicDef[PKey, aisClassAPositionReport](
      TopicName("sea_vessel_position_reports"),
      AvroCodec[aisClassAPositionReport])
  val vesselTopic   = ctx.topic(vessel)
  val vesselChannel = vesselTopic.akkaChannel

  test("akka stream committableSink") {
    val run = vesselChannel
      .withConsumerSettings(_.withClientId("c-id"))
      .withCommitterSettings(_.withParallelism(10))
      .source
      .map(m => vesselTopic.decoder(m).nullableDecode)
      .map(m =>
        vesselTopic.akkaProducerRecord(m.record.key(), m.record.value(), m.committableOffset))
      .take(2)
      .runWith(vesselChannel.committableSink)

    run.unsafeRunSync()
  }
  test("akka stream commitSink") {
    val run = vesselChannel
      .withConsumerSettings(_.withClientId("c-id"))
      .withCommitterSettings(_.withParallelism(10))
      .source
      .map(m => vesselTopic.decoder(m).nullableDecode)
      .map(m =>
        vesselTopic.akkaProducerRecord(m.record.key(), m.record.value(), m.committableOffset))
      .via(vesselChannel.flexiFlow)
      .map(_.passThrough)
      .take(2)
      .runWith(vesselChannel.commitSink)

    run.unsafeRunSync()
  }

  test("akka stream plainSink") {
    val run = vesselChannel
      .withConsumerSettings(_.withClientId("c-id"))
      .withCommitterSettings(_.withParallelism(10))
      .source
      .map(m => vesselTopic.decoder(m).nullableDecode)
      .map(m =>
        new org.apache.kafka.clients.producer.ProducerRecord(
          vessel.topicName.value,
          m.record.key(),
          m.record.value()))
      .take(2)
      .runWith(vesselChannel.plainSink)
    run.unsafeRunSync()
  }

  test("fs2 stream") {
    vesselChannel.stream
      .map(x => vesselTopic.decoder(x).decodeValue.record.value())
      .interruptAfter(3.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("assignment") {
    val datetime = LocalDateTime.now
    val ret = for {
      start <- vesselTopic.shortLiveConsumer.use(_.beginningOffsets)
      offsets = start.flatten[KafkaOffset].value.mapValues(_.value)
      _ <-
        vesselChannel
          .assign(offsets)
          .map(m => vesselTopic.decoder(m).decode)
          .take(1)
          .runWith(akkaSinks.ignore[IO])
    } yield ()
    ret.unsafeRunSync
  }
}
