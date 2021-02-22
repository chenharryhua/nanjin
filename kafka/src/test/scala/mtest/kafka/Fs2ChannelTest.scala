package mtest.kafka

import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.kafka._
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KJson}
import com.landoop.telecom.telecomitalia.telecommunications.{smsCallInternet, Key}
import fs2.kafka.AutoOffsetReset
import io.circe.generic.auto._
import org.apache.kafka.common.TopicPartition
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class Fs2ChannelTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  val backblaze_smart = TopicDef[KJson[lenses_record_key], String](TopicName("backblaze_smart"))
  val nyc_taxi_trip   = TopicDef[Array[Byte], trip_record](TopicName("nyc_yellow_taxi_trip_data"))

  val sms = TopicDef(
    TopicName("telecom_italia_data"),
    AvroCodec[Key](Key.schema).right.get,
    AvroCodec[smsCallInternet](smsCallInternet.schema).right.get)

  "FS2 Channel" - {
    "should be able to consume json topic" in {
      val topic = backblaze_smart
        .in(ctx)
        .fs2Channel
        .updateConsumer(_.withGroupId("fs2"))
        .updateProducer(_.withBatchSize(1))
        .updateConsumer(_.withAutoOffsetReset(AutoOffsetReset.Earliest))
      val ret =
        topic.stream
          .map(m => topic.decoder(m).tryDecodeKeyValue)
          .debug()
          .take(1)
          .interruptAfter(5.seconds)
          .compile
          .toList
      ret.asserting(_.size shouldBe 1)
    }

    "should be able to consume avro topic" in {
      val topic = ctx.topic(nyc_taxi_trip).fs2Channel
      val ret = topic
        .updateConsumer(_.withGroupId("g1"))
        .stream
        .map(m => topic.decoder(m).decodeValue)
        .debug()
        .take(1)
        .interruptAfter(5.seconds)
        .compile
        .toList
      ret.asserting(_.size shouldBe 1)
    }

    "should be able to consume telecom_italia_data topic" in {
      val topic = sms.in(ctx).fs2Channel.updateConsumer(_.withGroupId("g1"))
      val ret = topic
        .assign(KafkaTopicPartition(Map(new TopicPartition(topic.topicName.value, 0) -> KafkaOffset(0))))
        .map(m => topic.decoder(m).tryDecode)
        .map(_.toEither)
        .rethrow
        .take(1)
        .interruptAfter(5.seconds)
        .compile
        .toList
      ret.asserting(_.size shouldBe 1)
    }

    "should return empty when topic-partition is empty" in {
      val topic = sms.in(ctx).fs2Channel.updateConsumer(_.withGroupId("g1"))
      val ret = topic
        .assign(KafkaTopicPartition.emptyOffset)
        .map(m => topic.decoder(m).tryDecode)
        .map(_.toEither)
        .rethrow
        .interruptAfter(5.seconds)
        .compile
        .toList
      ret.asserting(_.isEmpty shouldBe true)
    }
  }
}
