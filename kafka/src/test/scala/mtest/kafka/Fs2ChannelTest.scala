package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, KUnknown, NJAvroCodec}
import com.landoop.telecom.telecomitalia.telecommunications.{smsCallInternet, Key}
import fs2.kafka.{AutoOffsetReset, CommittableProducerRecords, ProducerRecord, TransactionalProducerRecords}
import io.circe.generic.auto.*
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import eu.timepit.refined.auto.*

class Fs2ChannelTest extends AnyFunSuite {
  val backblaze_smart = TopicDef[KJson[lenses_record_key], String](TopicName("backblaze_smart"))
  val nyc_taxi_trip   = TopicDef[Array[Byte], trip_record](TopicName("nyc_yellow_taxi_trip_data"))

  val sms: TopicDef[Key, smsCallInternet] = TopicDef(
    TopicName("telecom_italia_data"),
    NJAvroCodec[Key](Key.schema).right.get,
    NJAvroCodec[smsCallInternet](smsCallInternet.schema).right.get)

  test("unknown schema") {
    val topic = ctx.topic[Array[Byte], KUnknown](nyc_taxi_trip.topicName)
    ctx
      .consume(topic.topicName)
      .stream
      .map(m => topic.decode(m))
      .take(1)
      .timeout(3.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("should be able to consume json topic") {
    val topic = backblaze_smart.in(ctx)
    val consumer =
      ctx
        .consume(topic.topicName)
        .updateConfig(_.withGroupId("fs2"))
        .updateConfig(_.withAutoOffsetReset(AutoOffsetReset.Earliest))

    val ret =
      consumer.stream
        .map(m => topic.decoder(m).tryDecodeKeyValue)
        .take(1)
        .map(_.show)
        .map(println)
        .timeout(3.seconds)
        .compile
        .toList
        .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("should be able to consume avro topic") {
    val topic    = ctx.topic(nyc_taxi_trip)
    val consumer = ctx.consume(topic.topicName).updateConfig(_.withGroupId("g1"))
    val ret = consumer.stream
      .map(m => topic.decoder(m).decodeValue)
      .take(1)
      .map(_.toString)
      .map(println)
      .timeout(3.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("should be able to consume telecom_italia_data topic") {
    val topic    = sms.in(ctx)
    val consumer = ctx.consume(topic.topicName).updateConfig(_.withGroupId("g1"))
    val ret = consumer
      .assign(KafkaTopicPartition(Map(new TopicPartition(topic.topicName.value, 0) -> KafkaOffset(0))))
      .map(m => topic.decoder(m).tryDecode)
      .map(_.toEither)
      .rethrow
      .take(1)
      .map(_.toString)
      .timeout(3.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("should return empty when topic-partition is empty") {
    val topic    = sms.in(ctx)
    val consumer = ctx.consume(topic.topicName).updateConfig(_.withGroupId("g1"))
    val ret = consumer
      .assign(KafkaTopicPartition.emptyOffset)
      .map(m => topic.decoder(m).tryDecode)
      .map(_.toEither)
      .rethrow
      .timeout(3.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.isEmpty)
  }

  test("transactional producer") {
    val src = sms.in(ctx)
    val txntopic = sms
      .in(ctx)
      .withTopicName("txn-target")
      .produce
      .updateConfig(_.withRetries(10))
      .transactional("txn")
      .updateConfig(_.withTransactionTimeout(10.seconds))
    val run = for {
      cr <- ctx.consume(src.topicName).stream.map(src.decoder(_).decode).take(10)
      producer <- txntopic.stream
      pr = TransactionalProducerRecords.one(
        CommittableProducerRecords.one[IO, Key, smsCallInternet](
          ProducerRecord("txn-target", cr.record.key, cr.record.value),
          cr.offset))
      _ <- fs2.Stream.eval(producer.produce(pr))
    } yield pr

    run.timeout(5.seconds).compile.drain.unsafeRunSync()
  }
}
