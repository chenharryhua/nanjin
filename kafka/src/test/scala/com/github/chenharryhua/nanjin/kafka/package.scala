package com.github.chenharryhua.nanjin

import cats.effect.{ContextShift, IO, Resource, Timer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

package object kafka {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext =
    KafkaSettings.local
      .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      .ioContext

  val taxi = ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data")

  val sparkSession: Resource[IO, SparkSession] =
    SparkSettings.default.updateConf(_.setMaster("local[*]").setAppName("test")).sessionResource[IO]
}
