package mtest.kafka

import akka.Done
import akka.kafka.ProducerMessage
import cats.effect.IO
import cats.implicits._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.funsuite.AnyFunSuite
import cats.derived.auto.show._
import io.chrisdavenport.cats.time._
import fs2.kafka.{ProducerRecord => Fs2ProducerRecord}
import scala.concurrent.duration._
import scala.util.Random
import org.apache.kafka.streams.scala.ImplicitConversions._
import fs2.kafka.ProducerRecords
import org.apache.kafka.clients.producer.ProducerRecord
import io.circe.generic.auto._
import cats.derived.auto.show._

case class AvroKey(key: String)
case class AvroValue(v1: String, v2: Int)

class ProducerTest extends AnyFunSuite {
  val srcTopic    = ctx.topic[AvroKey, AvroValue]("producer.test.source")
  val akkaTopic   = ctx.topic[AvroKey, AvroValue]("producer.test.akka")
  val fs2Topic    = ctx.topic[AvroKey, AvroValue]("producer.test.fs2")
  val streamTopic = ctx.topic[AvroKey, AvroValue]("producer.test.kafka")
  test("producer api") {

    val produceTask = (0 until 100).toList.traverse { i =>
      srcTopic.send(AvroKey(i.toString), AvroValue(Random.nextString(5), Random.nextInt(100)))
    }
    val srcChn  = srcTopic.akkaChannel(akkaSystem)
    val akkaChn = akkaTopic.akkaChannel(akkaSystem)

    val akkaTask: IO[Done] =
      srcChn
        .updateConsumerSettings(
          _.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .withGroupId("akka-task")
            .withCommitWarning(10.seconds))
        .consume
        .map(m => akkaTopic.decoder(m).decode)
        .map(m =>
          akkaTopic.description
            .akkaProducerRecords(m.record.key, m.record.value, m.committableOffset))
        .take(100)
        .runWith(akkaChn.committableSink)(materializer)

    val fs2Task: IO[Unit] = srcTopic.fs2Channel
      .updateConsumerSettings(
        _.withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest").withGroupId("fs2-task"))
      .consume
      .map(m => srcTopic.decoder(m).decode)
      .map(m => srcTopic.description.fs2ProducerRecords(m.record.key, m.record.value, m.offset))
      .take(100)
      .through(fs2.kafka.produce(fs2Topic.description.fs2ProducerSettings[IO]))
      .compile
      .drain

    val task = produceTask >> akkaTask >> fs2Task

    task.unsafeRunSync
  }
}
