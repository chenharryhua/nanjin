package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import org.scalatest.funsuite.AnyFunSuite

class SchemaRegistryTest extends AnyFunSuite {

  val nyc: TopicDef[Int, trip_record] =
    TopicDef[Int, trip_record](TopicName("nyc_yellow_taxi_trip_data"))

  val topic: KafkaTopic[IO, Int, trip_record] = nyc.in(ctx)

  test("compatiable test") {
    topic.schemaCompatibility.map(println).unsafeRunSync
  }
  test("register schema") {
    topic.schemaRegister.unsafeRunSync()
  }
}
