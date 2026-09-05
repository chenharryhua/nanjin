package com.github.chenharryhua.nanjin.kafka.connector

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.config.SerdeSettings
import com.github.chenharryhua.nanjin.kafka.{OptionalAvroSchemaPair, SchemaIncompatible, TopicName}
import fs2.kafka.{ProducerSettings, Serializer}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite

class ProduceGenericRecordTest extends AnyFunSuite {

  private val topic: TopicName = TopicName("produce.generic.record.test")

  private val stringSchema: Schema = Schema.create(Schema.Type.STRING)
  private val intSchema: Schema = Schema.create(Schema.Type.INT)

  private def baseSettings: ProducerSettings[IO, Array[Byte], Array[Byte]] =
    ProducerSettings[IO, Array[Byte], Array[Byte]](Serializer[IO, Array[Byte]], Serializer[IO, Array[Byte]])

  // A registry client with the topic's key/value subjects registered to the given schemas.
  private def registryWith(key: Schema, value: Schema): MockSchemaRegistryClient = {
    val client = new MockSchemaRegistryClient
    client.register(s"${topic.value}-key", new AvroSchema(key))
    client.register(s"${topic.value}-value", new AvroSchema(value))
    client
  }

  private def producer(
    caller: OptionalAvroSchemaPair,
    client: MockSchemaRegistryClient): ProduceGenericRecord[IO] =
    new ProduceGenericRecord[IO](
      topicName = topic,
      schemaPair = caller,
      srClient = client,
      serdeSettings = SerdeSettings(Map.empty),
      producerSettings = baseSettings)

  test("1.updateConfig transforms the underlying producer settings") {
    val p = producer(
      OptionalAvroSchemaPair(None, None),
      registryWith(stringSchema, intSchema)
    ).updateConfig(_.withClientId("nanjin-producer"))

    assert(p.properties.get("client.id").contains("nanjin-producer"))
  }

  test("2.updateConfig returns a distinct instance leaving the original unchanged") {
    val original = producer(OptionalAvroSchemaPair(None, None), registryWith(stringSchema, intSchema))
    val updated = original.updateConfig(_.withClientId("cid"))

    assert(!original.properties.contains("client.id"))
    assert(updated.properties.get("client.id").contains("cid"))
    assert(original ne updated)
  }

  test("3.schema resolves the write schema from the registry") {
    val p = producer(OptionalAvroSchemaPair(None, None), registryWith(stringSchema, intSchema))
    val resolved = p.schema.unsafeRunSync()
    assert(resolved.getType === Schema.Type.RECORD)
    assert(resolved.getName === "NJConsumerRecord")
  }

  test("4.schema raises SchemaIncompatible when caller schema is not backward-compatible with the registry") {
    // caller value is INT while the registered (broker) value is STRING: not backward-compatible
    val p = producer(
      OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), Some(AvroSchema(intSchema))),
      registryWith(stringSchema, stringSchema)
    )
    assertThrows[SchemaIncompatible](p.schema.unsafeRunSync())
  }

  test("5.absent registry schema is treated as compatible; caller schema is used") {
    // empty registry: no subjects registered, so the broker pair is (None, None)
    val emptyClient = new MockSchemaRegistryClient
    val p = producer(
      OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), Some(AvroSchema(intSchema))),
      emptyClient
    )
    // a missing broker schema is treated as compatible, so this resolves using the caller's schemas
    val resolved = p.schema.unsafeRunSync()
    assert(resolved.getType === Schema.Type.RECORD)
    assert(resolved.getName === "NJConsumerRecord")
  }
}
