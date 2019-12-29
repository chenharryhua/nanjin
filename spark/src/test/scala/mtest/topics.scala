package mtest

import cats.Show
import cats.implicits._
import cats.derived.auto.show._
import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import io.circe.generic.auto._
import mtest.kafka.trip_record

object topics {
  implicit val show: Show[ForTaskSerializable] = _.toString
  val taxi                                     = ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data")
  val pencil_topic                             = ctx.topic[Int, Pencil]("pencile")
  val first_topic                              = ctx.topic[Int, FirstStream]("first")
  val second_topic                             = ctx.topic[Int, SecondStream]("second")
  val ss                                       = ctx.topic[String, String]("ss")
  val si                                       = ctx.topic[String, Int]("si")
  val ii                                       = ctx.topic[Int, Int]("ii")

  val sparkafkaTopic: KafkaTopic[IO, Int, ForTaskSerializable] =
    ctx.topic[Int, ForTaskSerializable]("serializable.test")
}
