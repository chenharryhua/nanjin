package mtest.kafka

import akka.kafka.ProducerMessage
import cats.effect.IO
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJDateTimeRange}
import com.github.chenharryhua.nanjin.kafka.{stages, KafkaChannels, KafkaOffset, KafkaTopic}
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords, ProducerResult}
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import scala.concurrent.duration.DurationInt

class AkkaChannelTest extends AnyFunSuite {
  val topic: KafkaTopic[IO, Int, String] = ctx.topic[Int, String]("akka.consumer.test")

  val data: Stream[IO, ProducerResult[Int, String, Unit]] =
    Stream(
      ProducerRecords(List(
        ProducerRecord(topic.topicName.value, 1, "a"),
        ProducerRecord(topic.topicName.value, 2, "b"),
        ProducerRecord(topic.topicName.value, 3, "c"),
        ProducerRecord(topic.topicName.value, 4, "d"),
        ProducerRecord(topic.topicName.value, 5, "e")
      ))).covary[IO].through(topic.fs2Channel.producerPipe)

  data.compile.drain.unsafeRunSync()

  val akkaChannel: KafkaChannels.AkkaChannel[IO, Int, String] =
    topic.akkaChannel(akkaSystem).updateCommitterSettings(_.withParallelism(10).withParallelism(10))

  test("akka stream committableSink") {
    import org.apache.kafka.clients.producer.ProducerRecord
    val run = akkaChannel.source
      .map(m => topic.decoder(m).nullableDecode)
      .map(m =>
        ProducerMessage
          .single(new ProducerRecord(topic.topicName.value, m.record.key(), m.record.value()), m.committableOffset))
      .take(2)
      .runWith(akkaChannel.committableSink)

    run.unsafeRunSync()
  }
  test("akka stream flexiFlow/commitSink") {
    import org.apache.kafka.clients.producer.ProducerRecord
    val run = akkaChannel.source
      .map(m => topic.decoder(m).nullableDecode)
      .map(m =>
        ProducerMessage.single(
          new ProducerRecord(topic.topicName.value, m.record.key(), m.record.value()),
          m.committableOffset))
      .via(akkaChannel.flexiFlow)
      .map(_.passThrough)
      .take(2)
      .runWith(akkaChannel.commitSink)

    run.unsafeRunSync()
  }

  test("akka stream plainSink") {
    val run = akkaChannel.source
      .map(m => topic.decoder(m).nullableDecode)
      .map(m =>
        new org.apache.kafka.clients.producer.ProducerRecord(topic.topicName.value, m.record.key(), m.record.value()))
      .take(2)
      .runWith(akkaChannel.plainSink)
    run.unsafeRunSync()
  }
  test("akka source error") {
    val run = akkaChannel.source
      .map(m => topic.decoder(m).nullableDecode)
      .map(m => throw new Exception("oops"))
      .runWith(stages.ignore[IO])
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
        akkaChannel.assign(offsets).map(m => topic.decoder(m).decode).take(1).runWith(stages.ignore[IO])
    } yield ()
    ret.unsafeRunSync
  }
}
