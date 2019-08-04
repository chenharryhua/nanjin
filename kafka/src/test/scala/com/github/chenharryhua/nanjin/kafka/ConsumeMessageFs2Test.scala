package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import org.scalatest.FunSuite

class ConsumeMessageFs2Test extends FunSuite with ShowKafkaMessage with Fs2MessageBitraverse {
  val backblaze_smart = TopicDef[KJson[lenses_record_key], KJson[lenses_record]]("backblaze_smart")
  val nyc_taxi_trip   = TopicDef[Array[Byte], trip_record]("nyc_yellow_taxi_trip_data")
  test("should be able to consume json topic") {
    import io.circe.generic.auto._
    val ret = ctx
      .topic(backblaze_smart)
      .fs2Channel
      .consumeNativeMessages
      .map(_.bitraverse(identity, identity).toEither)
      .rethrow
      .take(3)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 3)
  }
  test("should be able to consume avro topic") {
    import cats.derived.auto.show._
    val ret = ctx
      .topic(nyc_taxi_trip)
      .fs2Channel
      .consumeValues
      .map(_.toEither)
      .rethrow
      .take(3)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 3)
  }

  test("should be able to consume payments topic") {
    val ret = ctx
      .topic[String, Payment]("cc_payments")
      .fs2Channel
      .consumeMessages
      .map(_.toEither)
      .rethrow
      .take(3)
      .compile
      .toList
      .unsafeRunSync()
    assert(ret.size == 3)
  }
}
