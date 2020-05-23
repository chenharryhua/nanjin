package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{akkaSinks, KafkaTopic, TopicName}
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka._
import com.landoop.transportation.nyc.trip.yellow.trip_record
import org.scalatest.funsuite.AnyFunSuite

class SparkExtTest extends AnyFunSuite {

  val topic: KafkaTopic[IO, String, trip_record] =
    ctx.topic[String, trip_record](TopicName("nyc_yellow_taxi_trip_data"))
  test("stream") {
    topic.sparKafka.fromKafka.flatMap(_.rdd.stream[IO].compile.drain).unsafeRunSync
  }
  test("source") {
    topic.sparKafka.fromKafka
      .flatMap(_.rdd.source[IO].map(println).take(10).runWith(akkaSinks.ignore[IO]))
      .unsafeRunSync
  }
}
