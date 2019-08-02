package com.github.chenharryhua.nanjin.kafka

import cats.derived.auto.show._
import cats.implicits._
import org.scalatest.FunSuite

class ConsumerApiTest extends FunSuite with ShowKafkaMessage {

  val nyc_taxi_trip: TopicDef[Array[Byte], trip_record] =
    TopicDef[Array[Byte], trip_record]("nyc_yellow_taxi_trip_data")

  val consumer = ctx.topic(nyc_taxi_trip).consumer

  test("consumer api") {
    consumer.numOfRecords.map(_.show).map(println).unsafeRunSync()
    consumer.retrieveFirstRecords.map(_.map(_.show).mkString("\n")).map(println).unsafeRunSync()
    consumer.retrieveFirstMessages.map(_.map(_.show).mkString("\n")).map(println).unsafeRunSync()
    consumer.retrieveLastRecords.map(_.map(_.show).mkString("\n")).map(println).unsafeRunSync()
    consumer.retrieveLastMessages.map(_.map(_.show).mkString("\n")).map(println).unsafeRunSync()
  }
}
