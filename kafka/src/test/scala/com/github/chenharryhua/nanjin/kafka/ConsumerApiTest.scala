package com.github.chenharryhua.nanjin.kafka

import java.time.LocalDateTime

import cats.derived.auto.show._
import cats.implicits._
import com.github.chenharryhua.nanjin.codec._
import org.scalatest.funsuite.AnyFunSuite

class ConsumerApiTest extends AnyFunSuite {

  val nyc_taxi_trip: TopicDef[Array[Byte], trip_record] =
    TopicDef[Array[Byte], trip_record]("nyc_yellow_taxi_trip_data")

  val consumer = ctx.topic(nyc_taxi_trip).consumer

  test("should be able to retrieve messages without error") {
    consumer.numOfRecords.map(_.show).unsafeRunSync()
    consumer.retrieveFirstRecords.map(_.map(_.show).mkString("\n")).unsafeRunSync()
    consumer.retrieveLastRecords.map(_.map(_.show).mkString("\n")).unsafeRunSync()
  }
  test("range for non-exist topic") {
    val topic = ctx.topic[Int, Int]("non-exist")
    val end   = LocalDateTime.now
    val start = end.minusHours(1)
    topic.consumer.offsetRangeFor(start, end).map(_.show).map(println).unsafeRunSync()
  }
}
