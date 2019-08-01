package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import cats.implicits._
import TopicDef._
import org.scalatest.FunSuite
import TopicDef._

class ConsumeMessageFs2Test extends FunSuite with ShowKafkaMessage with Fs2MessageBitraverse {

  test("consume json topic") {
    import io.circe.generic.auto._
    val jsonTopic =
      ctx.topic(TopicDef[KJson[lenses_record_key], KJson[lenses_record]]("backblaze_smart"))

    jsonTopic.fs2Stream.consumeNativeMessages
      .map(_.bitraverse(identity, identity).toEither)
      .rethrow
      .map(_.show)
      .showLinesStdOut
      .take(3)
      .compile
      .drain
      .unsafeRunSync()
  }
  test("consume avro topic") {
    import cats.derived.auto.show._
    val avroTopic =
      ctx.topic(TopicDef[Array[Byte], KAvro[trip_record]]("nyc_yellow_taxi_trip_data"))

    avroTopic.fs2Stream.consumeValues
      .map(_.toEither)
      .rethrow
      .map(_.show)
      .showLinesStdOut
      .take(3)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("payments") {
    val avroTopic =
      ctx.topic(TopicDef[String, KAvro[Payment]]("cc_payments"))

    avroTopic.fs2Stream.consumeMessages
      .map(_.toEither)
      .rethrow
      .map(_.show)
      .showLinesStdOut
      .take(3)
      .compile
      .drain
      .unsafeRunSync()
  }
}
