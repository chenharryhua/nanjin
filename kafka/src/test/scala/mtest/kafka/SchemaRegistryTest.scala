package mtest.kafka

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

class SchemaRegistryTest extends AnyFunSuite {
  val topicName: TopicName = TopicName("nyc_yellow_taxi_trip_data")

  val topic: AvroTopic[Int, trip_record] =
    AvroTopic[Int, trip_record](topicName)

  test("compatible") {
    val res = ctx.schemaRegistry.register(topic) >> ctx.isCompatible(topic)
    assert(res.unsafeRunSync())
  }

  test("incompatible") {
    val other = AvroTopic[String, String](topicName)
    assert(!ctx.isCompatible(other).unsafeRunSync())
  }
}
