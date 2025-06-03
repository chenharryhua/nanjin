package mtest.kafka

import cats.Endo
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toBifunctorOps
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.gr2Jackson
import eu.timepit.refined.auto.*
import fs2.kafka.AutoOffsetReset
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

object Fs2ChannelTestData {
  final case class Fs2Kafka(a: Int, b: String, c: Double)
  val topicDef: TopicDef[Int, Fs2Kafka] = TopicDef[Int, Fs2Kafka](TopicName("fs2.kafka.test"))
  val topic: KafkaTopic[IO, Int, Fs2Kafka] = ctx.topic(topicDef)
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
  test("should be able to consume avro topic") {

    val ret =
      ctx.schemaRegistry.register(topicDef).attempt >>
        ctx.produce[Int, Fs2Kafka].produceOne(topic.topicName.value, 1, Fs2Kafka(1, "a", 1.0)) >>
        ctx
          .consume(topicDef)
          .updateConfig(_.withGroupId("g1").withAutoOffsetReset(AutoOffsetReset.Earliest))
          .stream
          .take(1)
          .map(ccr => NJConsumerRecord(ccr.record).asJson)
          .timeout(3.seconds)
          .compile
          .toList
    assert(ret.unsafeRunSync().size == 1)
  }

  test("record format") {
    val ret =
      ctx
        .consume(topicDef)
        .stream
        .take(1)
        .map(_.record)
        .map(r => gr2Jackson(topic.topicDef.consumerFormat.toRecord(r)).get)
        .timeout(3.seconds)
        .compile
        .toList
        .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("serde") {
    ctx
      .consume(topic.topicName)
      .stream
      .take(1)
      .map { ccr =>
        topic.serde.deserialize(ccr)
        topic.serde.deserializeKey(ccr)
        topic.serde.deserializeValue(ccr.record)
        topic.serde.tryDeserialize(ccr.record)
        topic.serde.tryDeserializeKey(ccr.record)
        topic.serde.tryDeserializeValue(ccr.record)
        topic.serde.tryDeserializeKeyValue(ccr.record)
        topic.serde.optionalDeserialize(ccr.record)
        topic.serde.nullableDeserialize(ccr.record)
        topic.serde.nullableDeserialize(ccr.record.bimap[Array[Byte], Array[Byte]](_ => null, _ => null))
        val nj = topic.serde.toNJConsumerRecord(ccr).toNJProducerRecord.toProducerRecord
        topic.serde.serialize(nj)
      }
      .timeout(3.seconds)
      .compile
      .toList
      .unsafeRunSync()
  }

  test("produce") {
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
    ctx.publishJackson(jackson).flatMap(IO.println).unsafeRunSync()
  }

  private val cs: Endo[PureConsumerSettings] = _.withGroupId("nanjin")
    .withEnableAutoCommit(true)
    .withBootstrapServers("http://abc.com")
    .withProperty("abc", "efg")

  test("consumer config") {
    val consumer = ctx.consume(topicDef).updateConfig(cs).properties
    assert(consumer.get(ConsumerConfig.GROUP_ID_CONFIG).contains("nanjin"))
    assert(consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).contains("true"))
    assert(consumer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).contains("http://abc.com"))
    assert(consumer.get("abc").contains("efg"))
  }

  test("byte consumer config") {
    val consumer = ctx.consume("bytes").updateConfig(cs).properties
    assert(consumer.get(ConsumerConfig.GROUP_ID_CONFIG).contains("nanjin"))
    assert(consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).contains("true"))
    assert(consumer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).contains("http://abc.com"))
    assert(consumer.get("abc").contains("efg"))
  }

  private val ps: Endo[PureProducerSettings] =
    _.withClientId("nanjin").withBootstrapServers("http://abc.com").withProperty("abc", "efg")

  test("producer setting") {
    val producer = ctx.produce(topicDef.rawSerdes).updateConfig(ps).properties
    assert(producer.get(ConsumerConfig.CLIENT_ID_CONFIG).contains("nanjin"))
    assert(producer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).contains("http://abc.com"))
    assert(producer.get("abc").contains("efg"))
  }

  test("transactional producer setting") {
    val producer = ctx.produce(topicDef.rawSerdes).updateConfig(ps).transactional("trans").properties
    assert(producer.get(ConsumerConfig.CLIENT_ID_CONFIG).contains("nanjin"))
    assert(producer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).contains("http://abc.com"))
    assert(producer.get("abc").contains("efg"))
  }
}
