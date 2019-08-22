package com.github.chenharryhua.nanjin.kafka

import java.time.LocalDateTime

import cats.effect.IO
import org.scalatest.FunSuite
import frameless.cats.implicits._

class SerializableTest extends FunSuite {
  test("serizable") {
    val end   = LocalDateTime.now()
    val start = end.minusYears(1)
    val topic =
      SharedVariable(ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data"))
    val sharedTopic = SharedVariable(topic)
    sparkSession.use { implicit s =>
      SparkafkaDataset.sdataset(topic, start, end).flatMap(_.show[IO]())
    }.map(println).unsafeRunSync
  }
}
