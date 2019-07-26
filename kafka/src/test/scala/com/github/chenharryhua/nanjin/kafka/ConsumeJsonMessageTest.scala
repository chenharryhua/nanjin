package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import cats.implicits._
import io.circe.generic.auto._
import org.scalatest.FunSuite

class ConsumeJsonMessageTest extends FunSuite with ShowKafkaMessage with Fs2MessageBitraverse {

  test("consume json topic") {
    val jsonTopic =
      KafkaTopicName("backblaze_smart").in[KJson[lenses_record_key], KJson[lenses_record]](ctx)

    jsonTopic
      .fs2Stream[IO]
      .consumeMessages
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
    val avroTopic =
      KafkaTopicName("cc_payments").in[String, KAvro[Payment]](ctx)
      
    avroTopic
      .fs2Stream[IO]
      .consumeMessages
      .map(_.bitraverse(identity, identity).toEither)
      .rethrow
      .map(_.show)
      .showLinesStdOut
      .take(3)
      .compile
      .drain
      .unsafeRunSync()
  }
}
