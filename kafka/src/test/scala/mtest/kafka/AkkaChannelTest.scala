package mtest.kafka

import akka.kafka.{CommitterSettings, ConsumerMessage, ProducerMessage}
import akka.stream.scaladsl.Sink
import akka.Done
import akka.kafka.scaladsl.Committer
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJDateTimeRange}
import com.github.chenharryhua.nanjin.kafka.{stages, KafkaTopic, KafkaTopicPartition}
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords, ProducerResult}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt

class AkkaChannelTest extends AnyFunSuite {
  val topic: KafkaTopic[IO, Int, String] = ctx.topic[Int, String]("akka.consumer.test")

  val data: Stream[IO, ProducerResult[Unit, Int, String]] =
    Stream(
      ProducerRecords(List(
        ProducerRecord(topic.topicName.value, 1, "a"),
        ProducerRecord(topic.topicName.value, 2, "b"),
        ProducerRecord(topic.topicName.value, 3, "c"),
        ProducerRecord(topic.topicName.value, 4, "d"),
        ProducerRecord(topic.topicName.value, 5, "e")
      ))).covary[IO].through(topic.produce.pipe)

  data.compile.drain.unsafeRunSync()

  val akkaConsumer = topic.akka.comsume(akkaSystem)
  val akkaProducer = topic.akka.produce(akkaSystem)

  val commitSink: Sink[ProducerMessage.Envelope[Int, String, ConsumerMessage.Committable], Future[Done]] =
    akkaProducer.committableSink(CommitterSettings(akkaSystem))

  test("akka stream committableSink") {
    import org.apache.kafka.clients.producer.ProducerRecord
    val run = IO.fromFuture(
      IO(
        akkaConsumer.source
          .map(m => topic.decoder(m).nullableDecode)
          .map(m =>
            ProducerMessage.single(
              new ProducerRecord(topic.topicName.value, m.record.key(), m.record.value()),
              m.committableOffset))
          .take(2)
          .runWith(commitSink)))

    run.unsafeRunSync()
  }
  test("akka stream flexiFlow/commitSink") {
    import org.apache.kafka.clients.producer.ProducerRecord
    val run = IO.fromFuture(
      IO(
        akkaConsumer.source
          .map(m => topic.decoder(m).nullableDecode)
          .map(m =>
            ProducerMessage.single(
              new ProducerRecord(topic.topicName.value, m.record.key(), m.record.value()),
              m.committableOffset))
          .via(akkaProducer.flexiFlow)
          .map(_.passThrough)
          .take(2)
          .runWith(Committer.sink(CommitterSettings(akkaSystem)))))

    run.unsafeRunSync()
  }

  test("akka stream plainSink") {
    val run = IO.fromFuture(
      IO(
        akkaConsumer.source
          .map(m => topic.decoder(m).nullableDecode)
          .map(m =>
            new org.apache.kafka.clients.producer.ProducerRecord(
              topic.topicName.value,
              m.record.key(),
              m.record.value()))
          .take(2)
          .runWith(akkaProducer.plainSink)))
    run.unsafeRunSync()
  }
  test("akka source error") {
    val run =
      akkaConsumer.source
        .map(m => topic.decoder(m).nullableDecode)
        .map(_ => throw new Exception("oops"))
        .runWith(Sink.ignore)

    assertThrows[Exception](Await.result(run, 10.seconds))
  }

  test("assignment") {
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    val ret = for {
      range <- topic.shortLiveConsumer.use(_.offsetRangeFor(NJDateTimeRange(sydneyTime)))
    } yield {
      println(range)
      val start    = range.flatten.mapValues(_.from)
      val end      = range.flatten.mapValues(_.until)
      val distance = range.flatten.value.foldLeft(0L) { case (s, (_, r)) => s + r.distance }
      akkaConsumer
        .assign(start)
        .via(stages.takeUntilEnd(end))
        .map(m => topic.decoder(m).decode)
        .runFold(0L)((sum, _) => sum + 1)
        .map(n => assert(n == distance))
    }
    IO.fromFuture(ret).unsafeRunSync()
  }

  test("assignment - empty") {
    val ret =
      akkaConsumer
        .assign(KafkaTopicPartition.emptyOffset)
        .map(m => topic.decoder(m).decode)
        .runFold(0)((sum, _) => sum + 1)
    assert(Await.result(ret, 10.seconds) == 0)
  }
}
