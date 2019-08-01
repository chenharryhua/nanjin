package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import org.scalatest.FunSuite
import io.circe.generic.auto._
import KafkaTopicName._

class SchemaRegistryTest extends FunSuite {

  val topic =
    ctx.topic[Array[Byte], KAvro[trip_record]](KafkaTopicName("nyc_yellow_taxi_trip_data"))

  test("latest schema") {
    topic.schemaRegistry.latestMeta.map(_.show).map(println).unsafeRunSync()
  }
  test("compatiable test") {
    topic.schemaRegistry.testCompatibility.map(println).unsafeRunSync
  }
  ignore("register schema") {
    topic.schemaRegistry.register.unsafeRunSync()
  }
}
