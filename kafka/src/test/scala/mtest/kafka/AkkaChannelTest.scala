package mtest.kafka

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJDateTimeRange}
import com.github.chenharryhua.nanjin.kafka.{akkaSinks, KafkaChannels, KafkaOffset, KafkaTopic}
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerResult}
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import scala.concurrent.duration.DurationInt

class AkkaChannelTest extends AnyFunSuite {
  val topic: KafkaTopic[IO, Int, String] = ctx.topic[Int, String]("akka.consumer.test")

  val data: List[ProducerRecord[Int, String]] =
    List(topic.fs2PR(1, "a"), topic.fs2PR(2, "b"), topic.fs2PR(3, "c"), topic.fs2PR(4, "d"), topic.fs2PR(5, "e"))

  val sender: Stream[IO, List[ProducerResult[Int, String, Unit]]] =
    Stream.awakeEvery[IO](1.second).zipRight(Stream.eval(data.traverse(x => topic.send(x))))

  val akkaChannel: KafkaChannels.AkkaChannel[IO, Int, String] = topic.akkaChannel(akkaSystem)
  test("time-ranged") {
    val range = NJDateTimeRange(sydneyTime)
    (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >> IO.sleep(1.seconds)).unsafeRunSync()
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

  test("akka stream committableSink") {
    val run = akkaChannel
      .withConsumerSettings(_.withClientId("c-id"))
      .withCommitterSettings(_.withParallelism(10))
      .source
      .map(m => topic.decoder(m).nullableDecode)
      .map(m => topic.akkaProducerRecord(m.record.key(), m.record.value(), m.committableOffset))
      .take(2)
      .runWith(akkaChannel.committableSink)

    run.unsafeRunSync()
  }
  test("akka stream flexiFlow/commitSink") {
    val run = akkaChannel
      .withConsumerSettings(_.withClientId("c-id"))
      .withCommitterSettings(_.withParallelism(10))
      .source
      .map(m => topic.decoder(m).nullableDecode)
      .map(m => topic.akkaProducerRecord(m.record.key(), m.record.value(), m.committableOffset))
      .via(akkaChannel.flexiFlow)
      .map(_.passThrough)
      .take(2)
      .runWith(akkaChannel.commitSink)

    run.unsafeRunSync()
  }

  test("akka stream plainSink") {
    val run = akkaChannel
      .withConsumerSettings(_.withClientId("c-id"))
      .withCommitterSettings(_.withParallelism(10))
      .source
      .map(m => topic.decoder(m).nullableDecode)
      .map(m =>
        new org.apache.kafka.clients.producer.ProducerRecord(topic.topicName.value, m.record.key(), m.record.value()))
      .take(2)
      .runWith(akkaChannel.plainSink)
    run.unsafeRunSync()
  }
  test("akka source error") {
    val run = akkaChannel
      .withConsumerSettings(_.withClientId("c-id"))
      .withCommitterSettings(_.withParallelism(10))
      .source
      .map(m => topic.decoder(m).nullableDecode)
      .map(m => throw new Exception("oops"))
      .runWith(akkaSinks.ignore[IO])
    assertThrows[Exception](run.unsafeRunSync())
  }

  test("fs2 stream") {
    akkaChannel.stream
      .map(x => topic.decoder(x).decodeValue.record.value())
      .interruptAfter(3.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("assignment") {
    val datetime = LocalDateTime.now
    val ret = for {
      start <- topic.shortLiveConsumer.use(_.beginningOffsets)
      offsets = start.flatten[KafkaOffset].value.mapValues(_.value)
      _ <-
        akkaChannel.assign(offsets).map(m => topic.decoder(m).decode).take(1).runWith(akkaSinks.ignore[IO])
    } yield ()
    ret.unsafeRunSync
  }
}
