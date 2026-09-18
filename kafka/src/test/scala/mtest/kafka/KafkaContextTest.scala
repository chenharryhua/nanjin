package mtest.kafka

import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.kafka.config.{
  KafkaConsumerSettings,
  KafkaProducerSettings,
  KafkaSettings,
  KafkaStreamSettings,
  SerdeSettings
}
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, SchemaRegistryUrlAbsent}
import fs2.kafka.{
  AdminClientSettings,
  Deserializer,
  KeyDeserializer,
  KeySerializer,
  Serializer,
  ValueDeserializer,
  ValueSerializer
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class KafkaContextTest extends AnyFunSuite with Matchers {

  /** Settings with no broker/registry endpoints configured: enough to build a context and any operation that
    * does not force the schema registry client. Forcing the registry (an absent URL) throws
    * `SchemaRegistryUrlAbsent`.
    */
  private val emptySettings: KafkaSettings =
    KafkaSettings(
      KafkaConsumerSettings(Map.empty),
      KafkaProducerSettings(Map.empty),
      AdminClientSettings("broker-url"),
      KafkaStreamSettings(Map.empty),
      SerdeSettings(Map.empty)
    )

  private val ctx = KafkaContext[IO](emptySettings)

  private val byteKeyDeserializer: Resource[IO, KeyDeserializer[IO, Array[Byte]]] =
    Resource.pure(Deserializer[IO, Array[Byte]])
  private val byteValueDeserializer: Resource[IO, ValueDeserializer[IO, Array[Byte]]] =
    Resource.pure(Deserializer[IO, Array[Byte]])
  private val byteKeySerializer: Resource[IO, KeySerializer[IO, Array[Byte]]] =
    Resource.pure(Serializer[IO, Array[Byte]])
  private val byteValueSerializer: Resource[IO, ValueSerializer[IO, Array[Byte]]] =
    Resource.pure(Serializer[IO, Array[Byte]])

  test("1.consumeBytes does not require schema registry configuration") {
    noException shouldBe thrownBy(ctx.consumeBytes("raw-bytes"))
  }

  test("2.consume with explicit deserializers does not require schema registry configuration") {
    noException shouldBe thrownBy(ctx.consume("topic", byteKeyDeserializer, byteValueDeserializer))
  }

  test("3.produce with explicit serializers does not require schema registry configuration") {
    noException shouldBe thrownBy(ctx.produce("topic", byteKeySerializer, byteValueSerializer))
  }

  test("4.settings returns the settings the context was built with") {
    ctx.settings shouldBe emptySettings
  }

  test("5.updateConfig returns a new context with updated settings and leaves the original unchanged") {
    val updated = ctx.updateConfig(_.withBrokers("new-broker:9092"))
    // the new context reflects the update on a visible property
    updated.settings.consumerSettings.properties
      .get("bootstrap.servers") shouldBe Some("new-broker:9092")
    // the original context is immutable: its settings still carry no bootstrap.servers
    ctx.settings.consumerSettings.properties.get("bootstrap.servers") shouldBe None
  }

  test("6.schemaRegistry throws SchemaRegistryUrlAbsent when the registry URL is absent") {
    // schemaRegistry forces the lazy schema-registry client, which requires the URL config
    a[SchemaRegistryUrlAbsent] shouldBe thrownBy(ctx.schemaRegistry("topic"))
  }

  test("7.schemaRegistry builds a TopicSchemaRegistry when the registry URL is configured") {
    val configured =
      KafkaContext[IO](emptySettings.withSerdeProperty(_.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"))
    // constructing the (cached) registry client and the topic view performs no network I/O
    noException shouldBe thrownBy(configured.schemaRegistry("topic"))
  }
}
