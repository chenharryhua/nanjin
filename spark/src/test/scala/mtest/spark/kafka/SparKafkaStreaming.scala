package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.spark.kafka._
import com.landoop.transportation.nyc.trip.yellow.trip_record
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

@Ignore
class SparKafkaStreaming extends AnyFunSuite {
  test("streaming") {
    val topic = ctx.topic[String, trip_record](TopicName("nyc_yellow_taxi_trip_data"))
  }
}
