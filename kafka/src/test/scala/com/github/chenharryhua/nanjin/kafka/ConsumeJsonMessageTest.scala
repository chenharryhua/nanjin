package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import cats.implicits._
import io.circe.generic.auto._
import org.scalatest.FunSuite

class ConsumeJsonMessageTest extends FunSuite with ShowKafkaMessage {

  val topic =
    KafkaTopicName("backblaze_smart").in[KJson[lenses_record_key], KJson[lenses_record]](ctx)

  test("consume json") {
    topic
      .fs2Stream[IO]
      .consumeMessages
      // .filter(_.record.value().isFailure)
      .map(_.show)
      .showLinesStdOut
      .take(10)
      .compile
      .drain
      .unsafeRunSync()
  }
  ignore("akka consumer") {
    topic.akkaStream.consumeValidMessages
  }
}
