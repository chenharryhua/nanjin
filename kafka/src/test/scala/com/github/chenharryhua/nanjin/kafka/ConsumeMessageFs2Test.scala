package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import org.scalatest.FunSuite

class ConsumeMessageFs2Test extends FunSuite with ShowKafkaMessage with Fs2MessageBitraverse {
  val backblaze_smart = TopicDef[KJson[lenses_record_key], KJson[lenses_record]]("backblaze_smart")
  val nyc_taxi_trip   = TopicDef[Array[Byte], trip_record]("nyc_yellow_taxi_trip_data")
  test("consume json topic") {
    import io.circe.generic.auto._
    ctx
      .topic(backblaze_smart)
      .fs2Stream
      .consumeNativeMessages
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
    ctx
      .topic(nyc_taxi_trip)
      .fs2Stream
      .consumeValues
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
    ctx
      .topic[String, KAvro[Payment]]("cc_payments")
      .fs2Stream
      .consumeMessages
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
