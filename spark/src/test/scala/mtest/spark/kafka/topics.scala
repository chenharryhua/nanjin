package mtest.spark.kafka

import com.landoop.transportation.nyc.trip.yellow.trip_record
import mtest.spark.ctx

object topics {
  val taxi         = ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data")
  val pencil_topic = ctx.topic[Int, Pencil]("pencile")
  val first_topic  = ctx.topic[Int, FirstStream]("first")
  val second_topic = ctx.topic[Int, SecondStream]("second")
  val ss           = ctx.topic[String, String]("ss")
  val si           = ctx.topic[String, Int]("si")
  val ii           = ctx.topic[Int, Int]("ii")
}
