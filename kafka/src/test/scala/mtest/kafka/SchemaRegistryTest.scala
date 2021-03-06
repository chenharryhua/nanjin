package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaSettings, KafkaTopic, SchemaRegistrySettings, TopicDef}
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import monocle.function.At.at
import org.scalatest.funsuite.AnyFunSuite

class SchemaRegistryTest extends AnyFunSuite {
  val topicName: TopicName = TopicName("nyc_yellow_taxi_trip_data")

  val nyc: TopicDef[Int, trip_record] =
    TopicDef[Int, trip_record](topicName)

  val topic: KafkaTopic[IO, Int, trip_record] = nyc.in(ctx)

  test("compatiable") {
    val res = topic.schemaRegistry.testCompatibility.unsafeRunSync
    assert(res.isCompatible)
  }

  test("incompatiable") {
    val other = ctx.topic[String, String](topicName.value)
    val res   = other.schemaRegistry.testCompatibility.unsafeRunSync()
    assert(!res.isCompatible)
  }

  test("schema register is not configured") {
    val tmpCtx = KafkaSettings.schemaRegistrySettings
      .composeLens(SchemaRegistrySettings.config)
      .composeLens(at(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG))
      .set(None)(KafkaSettings.local)
      .ioContext
    assertThrows[Exception](nyc.in(tmpCtx).schemaRegistry.testCompatibility.unsafeRunSync())
  }

  test("schema register is not reachable") {
    val tmpCtx = KafkaSettings.schemaRegistrySettings
      .composeLens(SchemaRegistrySettings.config)
      .composeLens(at(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG))
      .set(Some("unknown-schema-register"))(KafkaSettings.local)
      .ioContext
    val res = nyc.in(tmpCtx).schemaRegistry.testCompatibility.unsafeRunSync()
    assert(res.key.isLeft)
    assert(res.value.isLeft)
  }

  test("register schema") {
    topic.schemaRegistry.register.unsafeRunSync()
  }
  test("retrieve schema") {
    println(ctx.schema(topic.topicName.value).unsafeRunSync)
  }
}
