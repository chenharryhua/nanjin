package mtest.kafka

import cats.effect.{IO, Resource}
import com.github.chenharryhua.nanjin.kafka.config.{
  KafkaConsumerSettings,
  KafkaProducerSettings,
  KafkaSettings,
  KafkaStreamSettings,
  SerdeSettings
}
import com.github.chenharryhua.nanjin.kafka.serdes.Primitive
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, SchemaRegistryUrlAbsent, TopicDef}
import fs2.kafka.{
  AdminClientSettings,
  Deserializer,
  KeyDeserializer,
  KeySerializer,
  Serializer,
  ValueDeserializer,
  ValueSerializer
}
import munit.FunSuite
import org.apache.avro.Schema
import org.apache.kafka.streams.StreamsConfig

import java.util.concurrent.atomic.AtomicBoolean

class KafkaContextTest extends FunSuite {

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
    val _ = ctx.consumeBytes("raw-bytes")
  }

  test("2.consume with explicit deserializers does not require schema registry configuration") {
    val _ = ctx.consume("topic", byteKeyDeserializer, byteValueDeserializer)
  }

  test("3.produce with explicit serializers does not require schema registry configuration") {
    val _ = ctx.produce("topic", byteKeySerializer, byteValueSerializer)
  }

  test("4.settings returns the settings the context was built with") {
    assertEquals(ctx.settings, emptySettings)
  }

  test("5.updateConfig returns a new context with updated settings and leaves the original unchanged") {
    val updated = ctx.updateConfig(_.withBrokers("new-broker:9092"))
    // the new context reflects the update on a visible property
    assertEquals(
      updated.settings.consumerSettings.properties.get("bootstrap.servers"),
      Some("new-broker:9092"))
    // the original context is immutable: its settings still carry no bootstrap.servers
    assertEquals(ctx.settings.consumerSettings.properties.get("bootstrap.servers"), None)
  }

  test("6.schemaRegistry throws SchemaRegistryUrlAbsent when the registry URL is absent") {
    // schemaRegistry forces the lazy schema-registry client, which requires the URL config
    intercept[SchemaRegistryUrlAbsent](ctx.schemaRegistry("topic"))
  }

  test("7.schemaRegistry builds a TopicSchemaRegistry when the registry URL is configured") {
    val configured =
      KafkaContext[IO](emptySettings.withSerdeProperty(_.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"))
    // constructing the (cached) registry client and the topic view performs no network I/O
    val _ = configured.schemaRegistry("topic")
  }

  test("8.serde, store, asKey, and asValue build offline for primitive serdes") {
    val configured =
      KafkaContext[IO](emptySettings.withSerdeProperty(_.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"))
    val topic = TopicDef("context-primitives", Primitive[String], Primitive[java.lang.Integer])

    val registered = configured.serde(topic)
    val stores = configured.store(topic)
    val key = configured.asKey(Primitive[String])
    val value = configured.asValue(Primitive[java.lang.Integer])

    assertEquals(registered.topicName, topic.topicName)
    assertEquals(stores.name, topic.topicName.value)
    assert(key.serde.serializer() != null)
    assert(value.serde.deserializer() != null)
  }

  test("9.typed, attempted, and generic connector constructors preserve client properties") {
    val settings = emptySettings
      .withBrokers("localhost:9092")
      .withSerdeProperty(_.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
      .withConsumerProperty(_.CLIENT_ID_CONFIG, "consumer-client")
      .withProducerProperty(_.CLIENT_ID_CONFIG, "producer-client")
    val configured = KafkaContext[IO](settings)
    val topic = TopicDef("context-connectors", Primitive[String], Primitive[java.lang.Integer])
    val string_schema = Schema.create(Schema.Type.STRING)
    val int_schema = Schema.create(Schema.Type.INT)

    val consume = configured.consume(topic)
    val attempt_consume = configured.attemptConsume(topic)
    val explicit_consume = configured.consume("explicit-consume", byteKeyDeserializer, byteValueDeserializer)
    val generic_consume_default = configured.consumeGenericRecord("generic-consume")
    val generic_consume_explicit =
      configured.consumeGenericRecord("generic-consume-explicit", Some(string_schema), Some(int_schema))
    val produce = configured.produce(topic)
    val explicit_produce = configured.produce("explicit-produce", byteKeySerializer, byteValueSerializer)
    val generic_produce_default = configured.produceGenericRecord("generic-produce")
    val generic_produce_explicit =
      configured.produceGenericRecord("generic-produce-explicit", Some(string_schema), Some(int_schema))

    List(consume, attempt_consume, explicit_consume, generic_consume_default, generic_consume_explicit)
      .foreach(c => assertEquals(c.properties.get("client.id"), Some("consumer-client")))
    List(produce, explicit_produce, generic_produce_default, generic_produce_explicit)
      .foreach(p => assertEquals(p.properties.get("client.id"), Some("producer-client")))
  }

  test("10.buildStreams builds its topology lazily and injects the application id") {
    val configured = KafkaContext[IO](
      emptySettings
        .withBrokers("localhost:9092")
        .withSerdeProperty(_.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"))
    val topology_built = new AtomicBoolean(false)
    val streams = configured.buildStreams("context-streams") { (_, _) =>
      topology_built.set(true)
    }

    assert(!topology_built.get())
    assertEquals(streams.properties(StreamsConfig.APPLICATION_ID_CONFIG), "context-streams")
    assert(streams.topology != null)
    assert(topology_built.get())
  }

  test("11.schema registry cache capacity accepts numeric values and falls back for invalid values") {
    val base = emptySettings.withSerdeProperty(_.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
    val numeric = KafkaContext[IO](base.withSerdeProperty(_.MAX_SCHEMAS_PER_SUBJECT_CONFIG, "25"))
    val invalid = KafkaContext[IO](base.withSerdeProperty(_.MAX_SCHEMAS_PER_SUBJECT_CONFIG, "not-a-number"))

    assert(numeric.schemaRegistry("numeric") != null)
    assert(invalid.schemaRegistry("invalid") != null)
  }
}
