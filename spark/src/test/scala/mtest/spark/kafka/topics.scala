package mtest.spark.kafka

import cats.implicits._
import com.landoop.transportation.nyc.trip.yellow.trip_record
import com.github.chenharryhua.nanjin.kafka.TopicName

object topics {
  val taxi         = ctx.topic[Int, trip_record](TopicName("nyc_yellow_taxi_trip_data"))
  val pencil_topic = ctx.topic[Int, Pencil](TopicName("pencile"))
  val first_topic  = ctx.topic[Int, FirstStream](TopicName("first"))
  val second_topic = ctx.topic[Int, SecondStream](TopicName("second"))
  val ss           = ctx.topic[String, String](TopicName("ss"))
  val si           = ctx.topic[String, Int](TopicName("si"))
  val ii           = ctx.topic[Int, Int](TopicName("ii"))
}
