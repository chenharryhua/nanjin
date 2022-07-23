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
    topic.consume.stream.map(m => topic.decode(m)).take(1).debug().compile.drain.unsafeRunSync()
  }

  test("should be able to consume json topic") {
    val topic = backblaze_smart.in(ctx)
    val consumer =
      topic.consume
        .updateConfig(_.withGroupId("fs2"))
        .updateConfig(_.withAutoOffsetReset(AutoOffsetReset.Earliest))

    val ret =
      consumer.stream
        .map(m => topic.decoder(m).tryDecodeKeyValue)
        .take(1)
        .map(_.show)
        .map(println)
        .interruptAfter(5.seconds)
        .compile
        .toList
        .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("should be able to consume avro topic") {
    val topic    = ctx.topic(nyc_taxi_trip)
    val consumer = topic.consume.updateConfig(_.withGroupId("g1"))
    val ret = consumer.stream
      .map(m => topic.decoder(m).decodeValue)
      .take(1)
      .map(_.toString)
      .map(println)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("should be able to consume telecom_italia_data topic") {
    val topic    = sms.in(ctx)
    val consumer = topic.consume.updateConfig(_.withGroupId("g1"))
    val ret = consumer
      .assign(KafkaTopicPartition(Map(new TopicPartition(topic.topicName.value, 0) -> KafkaOffset(0))))
      .map(m => topic.decoder(m).tryDecode)
      .map(_.toEither)
      .rethrow
      .take(1)
      .map(_.toString)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("should return empty when topic-partition is empty") {
    val topic    = sms.in(ctx)
    val consumer = topic.consume.updateConfig(_.withGroupId("g1"))
    val ret = consumer
      .assign(KafkaTopicPartition.emptyOffset)
      .map(m => topic.decoder(m).tryDecode)
      .map(_.toEither)
      .rethrow
      .interruptAfter(5.seconds)
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
      cr <- src.consume.stream.map(src.decoder(_).decode).take(10)
      producer <- txntopic.stream
      pr = TransactionalProducerRecords.one(
        CommittableProducerRecords.one[IO, Key, smsCallInternet](
          ProducerRecord("txn-target", cr.record.key, cr.record.value),
          cr.offset))
      _ <- fs2.Stream.eval(producer.produce(pr))
    } yield pr

    run.debug().compile.drain.unsafeRunSync()
  }
}
