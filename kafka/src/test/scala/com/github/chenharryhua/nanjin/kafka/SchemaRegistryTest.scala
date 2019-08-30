package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import org.scalatest.FunSuite
import cats.derived.auto.show._
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite

class SchemaRegistryTest extends AnyFunSuite {

  val nyc: TopicDef[Int, trip_record] =
    TopicDef[Int, trip_record]("nyc_yellow_taxi_trip_data")

  val topic: KafkaTopic[IO, Int, trip_record] =
    ctx.topic[Int, trip_record](nyc)

  ignore("latest schema") {
    topic.schemaRegistry.latestMeta.map(_.show).unsafeRunSync()
  }
  ignore("compatiable test") {
    topic.schemaRegistry.testCompatibility.map(println).unsafeRunSync
  }
  ignore("register schema") {
    topic.schemaRegistry.register.unsafeRunSync()
  }
  ignore("schema registry is not necessarily configured if it is not used") {
    val noRegistry = KafkaSettings.empty.withBrokers("localhost:9092").ioContext
    val topic      = noRegistry.topic[Int, Int]("no_schema_registry_test")
    topic.producer.send(1, 1).unsafeRunSync()
  }
}
