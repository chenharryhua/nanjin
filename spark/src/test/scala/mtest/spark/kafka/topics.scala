package mtest.spark.kafka

import cats.Show
import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import cats.implicits._
import cats.derived.auto.show._
import io.circe.generic.auto._

object topics {
  implicit val show: Show[ForTaskSerializable] = _.toString
  val taxi                                     = ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data")
  val pencil_topic                             = ctx.topic[Int, Pencil]("pencile")
  val first_topic                              = ctx.topic[Int, FirstStream]("first")
  val second_topic                             = ctx.topic[Int, SecondStream]("second")
  val ss                                       = ctx.topic[String, String]("ss")
  val si                                       = ctx.topic[String, Int]("si")
  val ii                                       = ctx.topic[Int, Int]("ii")
}
