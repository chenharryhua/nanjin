package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import org.scalatest.FunSuite
import io.circe.generic.auto._

class SchemaRegistryTest extends FunSuite {

  val topic =
    KafkaTopicName("nyc_yellow_taxi_trip_data").in[Int, KAvro[Payment]](ctx)
  ignore("latest schema") {
    topic.schemaRegistry[IO].latestMeta.map(_.show).map(println).unsafeRunSync()
  }
  test("compatiable test") {
    topic.schemaRegistry[IO].testCompatibility.map(x => println(x.show)).unsafeRunSync
  }
  ignore("register schema") {
    topic.schemaRegistry[IO].register.unsafeRunSync()
  }
}
