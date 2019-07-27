package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import org.scalatest.FunSuite
import io.circe.generic.auto._

class SchemaRegistryTest extends FunSuite {

  val topic =
    KafkaTopicName("nyc_yellow_taxi_trip_data").in[Array[Byte], KAvro[trip_record]](ctx)
  ignore("latest schema") {
    topic.schemaRegistry[IO].latestMeta.map(println).unsafeRunSync()
  }
  test("compatiable test") {
    topic.schemaRegistry[IO].testCompatibility.map(println).unsafeRunSync
  }
  test("register schema") {
    topic.schemaRegistry[IO].register.unsafeRunSync()
  }
}
