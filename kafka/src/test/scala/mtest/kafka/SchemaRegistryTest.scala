package mtest.kafka

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.kafka._
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import monocle.function.At.at
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class SchemaRegistryTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  val topicName: TopicName = TopicName("nyc_yellow_taxi_trip_data")

  val nyc: TopicDef[Int, trip_record] =
    TopicDef[Int, trip_record](topicName)

  val topic: KafkaTopic[IO, Int, trip_record] = nyc.in(ctx)

  "Schema Registry" - {
    "compatiable" in {
      val res = topic.schemaRegistry.testCompatibility
      res.asserting(_.isCompatible shouldBe true)
    }

    "incompatiable" in {
      val other = ctx.topic[String, String](topicName.value)
      val res   = other.schemaRegistry.testCompatibility
      res.asserting(_.isCompatible shouldBe false)
    }

    "schema register is not configured" in {
      val tmpCtx = KafkaSettings.schemaRegistrySettings
        .composeLens(SchemaRegistrySettings.config)
        .composeLens(at(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG))
        .set(None)(KafkaSettings.local)
        .ioContext
      nyc.in(tmpCtx).schemaRegistry.testCompatibility.assertThrows[Exception]
    }

    "schema register is not reachable" in {
      val tmpCtx = KafkaSettings.schemaRegistrySettings
        .composeLens(SchemaRegistrySettings.config)
        .composeLens(at(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG))
        .set(Some("unknown-schema-register"))(KafkaSettings.local)
        .ioContext
      val res = nyc.in(tmpCtx).schemaRegistry.testCompatibility
      res.asserting(_.key.isLeft shouldBe true)
      res.asserting(_.value.isLeft shouldBe true)
    }

    "register schema" in {
      topic.schemaRegistry.register.assertNoException
    }

    "retrieve schema" in {
      ctx.schema(topic.topicName.value).assertNoException
    }
  }
}
