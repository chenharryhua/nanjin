package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import org.scalatest.FunSuite
import io.circe.generic.auto._
import KafkaTopicName._

class SchemaRegistryTest extends FunSuite {

  val topic =
    KafkaTopicName("nyc_yellow_taxi_trip_data").in[IO, Array[Byte], KAvro[trip_record]](ctx)

  test("latest schema") { 
    topic.schemaRegistry[IO].latestMeta.map(_.show).map(println).unsafeRunSync()
  }
  test("compatiable test") {
    topic.schemaRegistry[IO].testCompatibility.map(println).unsafeRunSync
  }
  ignore("register schema") {
    topic.schemaRegistry[IO].register.unsafeRunSync()
  }
}
