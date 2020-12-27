package mtest.kafka

import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka._
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KJson}
import com.landoop.telecom.telecomitalia.telecommunications.{smsCallInternet, Key}
import fs2.kafka.AutoOffsetReset
import io.circe.generic.auto._
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite
import scala.concurrent.duration._

class Fs2ChannelTest extends AnyFunSuite {
  val backblaze_smart = TopicDef[KJson[lenses_record_key], String](TopicName("backblaze_smart"))
  val nyc_taxi_trip   = TopicDef[Array[Byte], trip_record](TopicName("nyc_yellow_taxi_trip_data"))

  val sms = TopicDef(
    TopicName("telecom_italia_data"),
    AvroCodec[Key](Key.schema).right.get,
    AvroCodec[smsCallInternet](smsCallInternet.schema).right.get)

  test("should be able to consume json topic") {
    val topic = backblaze_smart.in(ctx)
    val ret =
      topic.fs2Channel
        .withProducerSettings(_.withBatchSize(1))
        .withConsumerSettings(_.withAutoOffsetReset(AutoOffsetReset.Earliest))
        .stream
        .map(m => topic.decoder(m).tryDecodeKeyValue)
        .take(1)
        .map(_.show)
        .map(println)
        .interruptAfter(2.seconds)
        .compile
        .toList
        .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("should be able to consume avro topic") {
    val topic = ctx.topic(nyc_taxi_trip)
    val ret = topic.fs2Channel.stream
      .map(m => topic.decoder(m).decodeValue)
      .take(1)
      .map(_.show)
      .map(println)
      .interruptAfter(2.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 1)
  }

  test("should be able to consume telecom_italia_data topic") {
    val topic = sms.in(ctx)
    val ret = topic.fs2Channel
      .assign(Map(new TopicPartition(topic.topicName.value, 0) -> 0))
      .map(m => topic.decoder(m).tryDecode)
      .map(_.toEither)
      .rethrow
      .take(1)
      .map(_.show)
      .interruptAfter(2.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 1)
  }
}
