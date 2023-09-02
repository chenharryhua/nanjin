package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef}
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite
class SchemaRegistryTest extends AnyFunSuite {
  val topicName: TopicName = TopicName("nyc_yellow_taxi_trip_data")

  val nyc: TopicDef[Int, trip_record] =
    TopicDef[Int, trip_record](topicName)

  val topic: KafkaTopic[IO, Int, trip_record] = nyc.in(ctx)

  test("compatible") {
    val res = ctx.schemaRegistry.fetchAvroSchema(topic.topicName).unsafeRunSync()
    assert(res.isBackwardCompatible(nyc.schemaPair))

  }

  test("incompatible") {
    val other = ctx.topic[String, String](topicName)
    val res   = ctx.schemaRegistry.fetchAvroSchema(topicName).unsafeRunSync()
    assert(!res.isBackwardCompatible(other.topicDef.schemaPair))
  }

  test("register schema") {
    val topic = TopicDef[Int, Int](TopicName("test.register.schema"))
    val report = ctx.schemaRegistry.delete(topic.topicName) >>
      ctx.schemaRegistry.register(topic) >>
      ctx.schemaRegistry.fetchAvroSchema(topic.topicName)
    assert(report.unsafeRunSync().isIdentical(topic.schemaPair))
  }

  test("retrieve schema") {
    println(ctx.schemaRegistry.metaData(topic.topicName).unsafeRunSync())
    println(ctx.schemaRegistry.fetchAvroSchema(topic.topicName).unsafeRunSync())
  }
}
