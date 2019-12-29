package mtest.kafka

import cats.derived.auto.show._
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec._
import com.github.chenharryhua.nanjin.kafka.codec.show._
import com.github.chenharryhua.nanjin.kafka._
import com.github.chenharryhua.nanjin.kafka.codec.{KJson, ManualAvroSchema}
import com.landoop.telecom.telecomitalia.telecommunications.{smsCallInternet, Key}
import fs2.kafka.AutoOffsetReset
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite

class ConsumeMessageFs2Test extends AnyFunSuite {
  val backblaze_smart = TopicDef[KJson[lenses_record_key], String]("backblaze_smart")
  val nyc_taxi_trip   = TopicDef[Array[Byte], trip_record]("nyc_yellow_taxi_trip_data")

  val sms = TopicDef(
    "telecom_italia_data",
    ManualAvroSchema[Key](Key.schema),
    ManualAvroSchema[smsCallInternet](smsCallInternet.schema))

  test("should be able to consume json topic") {
    val topic = backblaze_smart.in(ctx)
    val ret =
      topic.fs2Channel
        .updateConsumerSettings(_.withAutoOffsetReset(AutoOffsetReset.Earliest))
        .consume
        .map(m => topic.decoder(m).tryDecodeKeyValue)
        .take(3)
        .map(_.show)
        .map(println)
        .compile
        .toList
        .unsafeRunSync()
    assert(ret.size == 3)
  }

  test("should be able to consume avro topic") {
    val topic = ctx.topic(nyc_taxi_trip)
    val ret = topic.fs2Channel.consume
      .map(m => topic.decoder(m).decodeValue)
      .take(3)
      .map(_.show)
      .map(println)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 3)
  }

  test("should be able to consume telecom_italia_data topic") {
    val topic = sms.in(ctx)
    val ret = topic.fs2Channel.consume
      .map(m => topic.decoder(m).tryDecode)
      .map(_.toEither)
      .rethrow
      .take(3)
      .map(_.show)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 3)
  }
}
