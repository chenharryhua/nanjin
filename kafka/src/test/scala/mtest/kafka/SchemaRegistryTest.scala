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

  val topic: KafkaTopic[IO, Int, trip_record] = ctx.topic(nyc)

  test("compatible") {
    val res = (ctx.schemaRegistry.register(nyc) >>
      ctx.schemaRegistry.metaData(nyc.topicName) >>
      ctx.schemaRegistry.fetchAvroSchema(topic.topicName)).unsafeRunSync()
    assert(res.isFullCompatible(nyc.schemaPair))
  }

  test("incompatible") {
    val other = ctx.topic(TopicDef[String, String](topicName))
    val res = ctx.schemaRegistry.fetchAvroSchema(topicName).unsafeRunSync()
    assert(res.backward(other.topicDef.schemaPair).nonEmpty)
    assert(res.forward(other.topicDef.schemaPair).nonEmpty)
  }

  test("register schema should be identical") {
    val topic = TopicDef[reddit_post, reddit_post](TopicName("test.register.schema"))
    val report = ctx.schemaRegistry.delete(topic.topicName).attempt >>
      ctx.schemaRegistry.register(topic) >>
      ctx.schemaRegistry.fetchAvroSchema(topic.topicName)
    assert(report.unsafeRunSync().isIdentical(topic.schemaPair))
    assert(report.unsafeRunSync().isFullCompatible(topic.schemaPair))
  }

  test("compatibility") {
    val other = TopicDef[Int, reddit_post](TopicName("abc")).schemaPair
    val skm = topic.topicDef.schemaPair
    assert(other.forward(skm).nonEmpty)
    assert(other.backward(skm).nonEmpty)
  }
}
