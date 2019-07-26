package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import org.scalatest.FunSuite

class SchemaRegistryTest extends FunSuite {
  val topic = KafkaTopicName("cc_payments").in[String, KAvro[Payment]](ctx)
  test("latest schema") {
    topic.latestSchema[IO].map(_.show).map(println).unsafeRunSync()
  }
  test("compatiable test") {
    topic
      .testSchemaCompat[IO]
      .map(println)
      .unsafeRunSync
  }
  ignore("register schema") {
    topic.registerSchema[IO].unsafeRunSync()
  }
}
