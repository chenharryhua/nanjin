package mtest
import cats.implicits._
import cats.derived.auto.show._

object topics {
  val taxi         = ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data")
  val pencil_topic = ctx.topic[Int, Pencil]("pencile")
  val first_topic  = ctx.topic[Int, FirstStream]("first")
  val second_topic = ctx.topic[Int, SecondStream]("second")
}
