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
    val res = (ctx.schemaRegistry.register(topic) >>
      ctx.schemaRegistry.metaData(topic.topicName) >>
      ctx.schemaRegistry.fetchOptionalAvroSchema(topic.topicName)).unsafeRunSync()
    assert(res.toPair.isFullCompatible(topic.pair.optionalAvroSchemaPair.toPair))
  }

  test("incompatible") {
    val other = AvroTopic[String, String](topicName)
    val res = ctx.schemaRegistry.fetchOptionalAvroSchema(topicName).unsafeRunSync()
    assert(res.toPair.backward(other.pair.optionalAvroSchemaPair.toPair).nonEmpty)
    assert(res.toPair.forward(other.pair.optionalAvroSchemaPair.toPair).nonEmpty)
  }

  test("register schema should be identical") {
    val topic = AvroTopic[reddit_post, reddit_post](TopicName("test.register.schema"))
    val report = ctx.schemaRegistry.delete(topic.topicName).attempt >>
      ctx.schemaRegistry.register(topic) >>
      ctx.schemaRegistry.fetchAvroSchema(topic.topicName)
    assert(report.unsafeRunSync().isIdentical(topic.pair.optionalAvroSchemaPair.toPair))
    assert(report.unsafeRunSync().isFullCompatible(topic.pair.optionalAvroSchemaPair.toPair))
  }

  test("compatibility") {
    val other = AvroTopic[Int, reddit_post](TopicName("abc")).pair.optionalAvroSchemaPair.toPair
    val skm = topic.pair.optionalAvroSchemaPair.toPair
    assert(other.forward(skm).nonEmpty)
    assert(other.backward(skm).nonEmpty)
  }
}
