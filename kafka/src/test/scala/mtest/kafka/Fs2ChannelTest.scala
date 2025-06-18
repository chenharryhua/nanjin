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
        ctx.produce[Int, Fs2Kafka].produceOne(topicDef.topicName.value, 1, Fs2Kafka(1, "a", 1.0)) >>
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
        .map(r => gr2Jackson(topicDef.consumerFormat.toRecord(r)).get)
        .timeout(3.seconds)
        .compile
        .toList
        .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("serde") {
    val serde = ctx.serde(topicDef)
    ctx
      .consume(topicDef.topicName)
      .stream
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
    val producer = ctx.produce(topicDef.codecPair).updateConfig(ps).properties
    assert(producer.get(ConsumerConfig.CLIENT_ID_CONFIG).contains("nanjin"))
    assert(producer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).contains("http://abc.com"))
    assert(producer.get("abc").contains("efg"))
  }

  test("transactional producer setting") {
    val producer = ctx.produce(topicDef.codecPair).updateConfig(ps).transactional("trans").properties
    assert(producer.get(ConsumerConfig.CLIENT_ID_CONFIG).contains("nanjin"))
    assert(producer.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).contains("http://abc.com"))
    assert(producer.get("abc").contains("efg"))
  }

  test("generic record from") {
    ctx
      .consume("telecom_italia_data")
      .updateConfig(_.withMaxPollRecords(10))
      .genericRecords(1, 10)
      .take(50)
      .debug()
      .foreach(ccr => IO(assert(ccr.record.partition == 1)).void)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("assign") {
    ctx
      .consume(topicDef)
      .updateConfig(_.withMaxPollRecords(5))
      .assign(0, 0)
      .take(1)
      .debug()
      .foreach(ccr => IO(assert(ccr.record.partition == 0)).void)
      .compile
      .drain
      .unsafeRunSync()
  }
}
