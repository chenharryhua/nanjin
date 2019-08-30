package com.github.chenharryhua.nanjin

import cats.effect.{ContextShift, IO, Resource, Timer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import cats.derived.auto.show._
import scala.concurrent.ExecutionContext.Implicits.global
import cats.implicits._
package object kafka {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext =
    KafkaSettings.local
      .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .ioContext

  val taxi = ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data")
}
