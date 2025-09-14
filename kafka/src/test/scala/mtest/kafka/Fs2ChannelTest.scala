package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toBifunctorOps
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.gr2Jackson
import eu.timepit.refined.auto.*
import fs2.kafka.{Acks, AutoOffsetReset}
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

object Fs2ChannelTestData {
  final case class Fs2Kafka(a: Int, b: String, c: Double)
  val topicDef: TopicDef[Int, Fs2Kafka] = TopicDef[Int, Fs2Kafka](TopicName("fs2.kafka.test"))
  val jackson =
    """
      {
      "partition" : 0,
      "offset" : 0,
      "timestamp" : 1696207641300,
      "key" : {
        "int" : 1
      },
      "value" : {
        "mtest.kafka.Fs2ChannelTestData.Fs2Kafka" : {
          "a" : 1,
          "b" : "a",
          "c" : 1.0
        }
      },
      "topic" : "whatever",
      "timestampType" : 0,
      "headers" : [
      ]
    }
     """

  val json =
    """
      {
      "partition" : 0,
      "offset" : 0,
      "timestamp" : 1696207641300,
      "key" : 1,
      "value" : {
        "a" : 1,
        "b" : "a",
        "c" : 1.0
      },
      "topic" : "don't care",
      "timestampType" : 0,
      "headers" : [
      ]
    } """
}

class Fs2ChannelTest extends AnyFunSuite {
  import Fs2ChannelTestData.*
  test("1.should be able to consume avro topic") {

    val ret =
      ctx.schemaRegistry.register(topicDef).attempt >>
        ctx.produce[Int, Fs2Kafka].produceOne(topicDef.topicName.name, 1, Fs2Kafka(1, "a", 1.0)) >>
        ctx
          .consume(topicDef)
          .updateConfig(_.withGroupId("g1").withAutoOffsetReset(AutoOffsetReset.Earliest))
          .subscribe
          .take(1)
          .map(ccr => NJConsumerRecord(ccr.record).asJson)
          .timeout(3.seconds)
          .compile
          .toList
    assert(ret.unsafeRunSync().size == 1)
  }

  test("2.record format") {
    val ret =
      ctx
        .consume(topicDef)
        .subscribe
        .take(1)
        .map(_.record)
        .map(r => gr2Jackson(topicDef.consumerFormat.toRecord(r)).get)
        .timeout(3.seconds)
        .compile
        .toList
        .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("3.serde") {
    val serde = ctx.serde(topicDef)
    ctx
      .consume(topicDef.topicName.name)
      .subscribeBytes
      .take(1)
      .map { ccr =>
        serde.deserialize(ccr)
        serde.deserializeKey(ccr)
        serde.deserializeValue(ccr.record)
        serde.tryDeserialize(ccr.record)
        serde.tryDeserializeKey(ccr.record)
        serde.tryDeserializeValue(ccr.record)
        serde.tryDeserializeKeyValue(ccr.record)
        serde.optionalDeserialize(ccr.record)
        serde.nullableDeserialize(ccr.record)
        serde.nullableDeserialize(ccr.record.bimap[Array[Byte], Array[Byte]](_ => null, _ => null))
        val nj = serde.toNJConsumerRecord(ccr).toNJProducerRecord.toProducerRecord
        serde.serialize(nj)
      }
      .timeout(3.seconds)
      .compile
      .toList
      .unsafeRunSync()
  }

  test("4.produce") {
    val jackson =
      """
      {
      "partition" : 0,
      "offset" : 0,
      "timestamp" : 1696207641300,
      "key" : {
        "int" : 2
      },
      "value" : {
        "mtest.kafka.Fs2ChannelTestData.Fs2Kafka" : {
          "a" : 2,
          "b" : "b",
          "c" : 2.0
        }
      },
      "serializedKeySize":null,
      "serializedValueSize":null,
      "topic" : "fs2.kafka.test",
      "timestampType" : 0,
      "headers" : [
      ],
      "leaderEpoch":null
    }
     """
    ctx.produce("fs2.kafka.test").jackson(jackson).flatMap(IO.println).unsafeRunSync()
  }

  test("5.consumer config") {
    val consumer = ctx
      .consume(topicDef)
      .updateConfig(
        _.withGroupId("nanjin")
          .withEnableAutoCommit(true)
          .withBootstrapServers("http://abc.com")
          .withProperty("abc", "efg"))
      .updateConfig(_.withAutoOffsetReset(AutoOffsetReset.Earliest))
      .properties
    assert(consumer.get(ConsumerConfig.GROUP_ID_CONFIG).contains("nanjin"))
    assert(consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).contains("true"))
    assert(consumer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).contains("http://abc.com"))
    assert(consumer.get("abc").contains("efg"))
    assert(consumer.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).contains("earliest"))
  }

  test("6.byte consumer config") {
    val consumer = ctx
      .consume("bytes")
      .updateConfig(
        _.withGroupId("nanjin")
          .withEnableAutoCommit(true)
          .withBootstrapServers("http://abc.com")
          .withProperty("abc", "efg"))
      .updateConfig(_.withAutoOffsetReset(AutoOffsetReset.Earliest))
      .properties
    assert(consumer.get(ConsumerConfig.GROUP_ID_CONFIG).contains("nanjin"))
    assert(consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).contains("true"))
    assert(consumer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).contains("http://abc.com"))
    assert(consumer.get("abc").contains("efg"))
    assert(consumer.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).contains("earliest"))
  }

  test("7.producer setting") {
    val producer =
      ctx
        .produce(topicDef.codecPair)
        .updateConfig(
          _.withClientId("nanjin").withBootstrapServers("http://abc.com").withProperty("abc", "efg")
        )
        .updateConfig(_.withAcks(Acks.Zero))
        .properties
    assert(producer.get(ConsumerConfig.CLIENT_ID_CONFIG).contains("nanjin"))
    assert(producer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).contains("http://abc.com"))
    assert(producer.get("abc").contains("efg"))
    assert(producer.get(ProducerConfig.ACKS_CONFIG).contains("0"))
  }

  test("8.transactional producer setting") {
    val producer = ctx
      .produce(topicDef.codecPair)
      .updateConfig(
        _.withClientId("nanjin").withBootstrapServers("http://abc.com").withProperty("abc", "efg")
      )
      .updateConfig(_.withAcks(Acks.Zero))
      .transactional("trans")
      .properties
    assert(producer.get(ConsumerConfig.CLIENT_ID_CONFIG).contains("nanjin"))
    assert(producer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).contains("http://abc.com"))
    assert(producer.get("abc").contains("efg"))
    assert(producer.get(ProducerConfig.ACKS_CONFIG).contains("0"))
  }

  test("9.generic record range - should stop") {
    val res = ctx
      .consume("telecom_italia_data")
      .updateConfig(_.withMaxPollRecords(10))
      .circumscribedStream(Map(0 -> (0L, 5L)))
      .flatMap(_.stream.map(_.record.value).debug())
      .compile
      .drain
      .as(true)

    assert(res.unsafeRunSync())
  }

  test("10.generic record manualCommitStream") {
    val res = ctx
      .consume("telecom_italia_data")
      .updateConfig(_.withMaxPollRecords(10))
      .manualCommitStream
      .flatMap(_.stream.map(_.record.value))
      .take(5)
      .debug()
      .compile
      .drain
      .as(true)
    assert(res.unsafeRunSync())
  }

  test("11.range - should stop") {
    val res = ctx
      .consume(topicDef)
      .updateConfig(_.withMaxPollRecords(10))
      .circumscribedStream(Map(0 -> (0L, 1L)))
      .flatMap(_.stream.map(_.record.value).debug())
      .compile
      .drain
      .as(true)
    assert(res.unsafeRunSync())
  }

  test("12.manualCommitStream") {
    val res = ctx
      .consume(topicDef)
      .updateConfig(_.withMaxPollRecords(10))
      .manualCommitStream
      .flatMap(_.stream.map(_.record.value))
      .take(1)
      .debug()
      .compile
      .drain
      .as(true)
    assert(res.unsafeRunSync())
  }

  test("13. generic record without schema registry") {
    val ret =
      ctx
        .consume(topicDef.topicName.name)
        .withSchema(
          _.withKeySchema(topicDef.schemaPair.key)
            .withValSchema(topicDef.schemaPair.value)
            .withoutKeySchema
            .withoutValSchema
            .replaceKeySchema(topicDef.schemaPair.key)
            .replaceValSchema(topicDef.schemaPair.value))
        .subscribe
        .take(1)
        .map(_.record)
        .debug()
        .timeout(3.seconds)
        .compile
        .toList
        .unsafeRunSync()
    assert(ret.size == 1)
    assert(ret.head.value.isSuccess)
  }
}
