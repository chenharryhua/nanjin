package com.github.chenharryhua.nanjin.kafka.connector

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.config.SerdeSettings
import com.github.chenharryhua.nanjin.kafka.{TopicName, TopicSchemaAbsent}
import fs2.kafka.{ProducerSettings, Serializer}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.Schema
import munit.CatsEffectSuite

class ProduceGenericRecordTest extends CatsEffectSuite {

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

  private def producer(client: MockSchemaRegistryClient): ProduceGenericRecord[IO] =
    new ProduceGenericRecord[IO](
      topicName = topic,
      srClient = client,
      serdeSettings = SerdeSettings(Map.empty),
      producerSettings = baseSettings)

  test("1.updateConfig transforms the underlying producer settings") {
    val p = producer(registryWith(stringSchema, intSchema))
      .updateConfig(_.withClientId("nanjin-producer"))

    assert(p.properties.get("client.id").contains("nanjin-producer"))
  }

  test("2.updateConfig returns a distinct instance leaving the original unchanged") {
    val original = producer(registryWith(stringSchema, intSchema))
    val updated = original.updateConfig(_.withClientId("cid"))

    assert(!original.properties.contains("client.id"))
    assert(updated.properties.get("client.id").contains("cid"))
    assert(original ne updated)
  }

  test("3.schema resolves the write schema from the registry") {
    val p = producer(registryWith(stringSchema, intSchema))
    p.schema.map { resolved =>
      assert(resolved.getType == Schema.Type.RECORD)
      assert(resolved.getName == "NJConsumerRecord")
    }
  }

  test("4.schema raises TopicSchemaAbsent when the registry is missing a subject") {
    // only the key subject is registered; the value subject is absent
    val client = new MockSchemaRegistryClient
    client.register(s"${topic.value}-key", new AvroSchema(stringSchema))
    val p = producer(client)
    interceptIO[TopicSchemaAbsent](p.schema)
  }
}
