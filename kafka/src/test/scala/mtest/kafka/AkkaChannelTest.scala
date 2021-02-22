package mtest.kafka

import akka.kafka.ProducerMessage
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJDateTimeRange}
import com.github.chenharryhua.nanjin.kafka.{stages, KafkaChannels, KafkaTopic, KafkaTopicPartition}
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords, ProducerResult}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class AkkaChannelTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  val topic: KafkaTopic[IO, Int, String] = ctx.topic[Int, String]("akka.consumer.test")

  val data: Stream[IO, ProducerResult[Unit, Int, String]] =
    Stream(
      ProducerRecords(List(
        ProducerRecord(topic.topicName.value, 1, "a"),
        ProducerRecord(topic.topicName.value, 2, "b"),
        ProducerRecord(topic.topicName.value, 3, "c"),
        ProducerRecord(topic.topicName.value, 4, "d"),
        ProducerRecord(topic.topicName.value, 5, "e")
      ))).covary[IO].through(topic.fs2Channel.producerPipe).debug()

  val akkaChannel: KafkaChannels.AkkaChannel[IO, Int, String] =
    topic.akkaChannel(akkaSystem).updateCommitter(_.withParallelism(10).withParallelism(10))

  "Akka Channel" - {
    "data ingestion" in {
      data.compile.drain.assertNoException
    }

    "committableSink" in {
      import org.apache.kafka.clients.producer.ProducerRecord
      val run = akkaChannel.source
        .map(m => topic.decoder(m).nullableDecode)
        .map(m =>
          ProducerMessage
            .single(new ProducerRecord(topic.topicName.value, m.record.key(), m.record.value()), m.committableOffset))
        .take(2)
        .runWith(akkaChannel.committableSink)

      run.assertNoException
    }

    "flexiFlow/commitSink" in {
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

      run.assertNoException
    }

    "plainSink" in {
      val run = akkaChannel.source
        .map(m => topic.decoder(m).nullableDecode)
        .map(m =>
          new org.apache.kafka.clients.producer.ProducerRecord(topic.topicName.value, m.record.key(), m.record.value()))
        .take(2)
        .runWith(akkaChannel.plainSink)
      run.assertNoException
    }

    "akka source error" in {
      val run = akkaChannel.source
        .map(m => topic.decoder(m).nullableDecode)
        .map(m => throw new Exception("oops"))
        .runWith(stages.ignore[IO])
      run.assertThrows[Exception]
    }

    "fs2 stream" in {
      val run =
        akkaChannel.stream.map(x => topic.decoder(x).decodeValue.record.value()).interruptAfter(3.seconds).compile.drain
      run.assertNoException
    }

    "assignment - date range" in {
      val datetime = LocalDateTime.now
      val ret = for {
        range <- topic.shortLiveConsumer.use(_.offsetRangeFor(NJDateTimeRange(sydneyTime)))
      } yield {
        println(range)
        val start    = range.flatten.mapValues(_.from)
        val end      = range.flatten.mapValues(_.until)
        val distance = range.flatten.value.foldLeft(0L) { case (s, (_, r)) => s + r.distance }
        akkaChannel
          .assign(start)
          .via(stages.takeUntilEnd(end))
          .map(m => topic.decoder(m).decode)
          .runFold(0L)((sum, _) => sum + 1)
          .map(n => assert(n == distance))
      }
      IO.fromFuture(ret)
    }

    "assignment - empty" in {
      val ret =
        akkaChannel
          .assign(KafkaTopicPartition.emptyOffset)
          .map(m => topic.decoder(m).decode)
          .runFold(0)((sum, _) => sum + 1)
      assert(Await.result(ret, 10.seconds) == 0)
    }
  }
}
