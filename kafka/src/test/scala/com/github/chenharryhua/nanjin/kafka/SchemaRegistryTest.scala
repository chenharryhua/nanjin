package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import org.scalatest.FunSuite
import io.circe.generic.auto._
import TopicDef._

class SchemaRegistryTest extends FunSuite {
  val nyc = TopicDef("nyc_yellow_taxi_trip_data")
  val topic =
    ctx.topic[Int, KAvro[trip_record]](nyc) 

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
