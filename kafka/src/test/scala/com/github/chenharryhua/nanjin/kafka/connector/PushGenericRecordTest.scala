package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.kafka.config.SerdeSettings
import com.github.chenharryhua.nanjin.kafka.record.NJConsumerRecord
import com.github.chenharryhua.nanjin.kafka.{AvroSchemaPair, TopicName}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.Record
import munit.FunSuite

import scala.jdk.CollectionConverters.*

class PushGenericRecordTest extends FunSuite {

  private val TOPIC: TopicName = TopicName("push.generic.record.unit.test")
  private val STRING_SCHEMA: Schema = Schema.create(Schema.Type.STRING)
  private val BYTES_SCHEMA: Schema = Schema.create(Schema.Type.BYTES)
  private val INT_SCHEMA: Schema = Schema.create(Schema.Type.INT)
  private val LONG_SCHEMA: Schema = Schema.create(Schema.Type.LONG)
  private val FLOAT_SCHEMA: Schema = Schema.create(Schema.Type.FLOAT)
  private val DOUBLE_SCHEMA: Schema = Schema.create(Schema.Type.DOUBLE)
  private val BOOLEAN_SCHEMA: Schema = Schema.create(Schema.Type.BOOLEAN)
  private val RECORD_SCHEMA: Schema =
    SchemaBuilder.record("Payload").namespace("mtest.kafka").fields().requiredString("name").endRecord()

  private def serdeSettings: SerdeSettings =
    SerdeSettings(
      Map(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "mock://push-generic-record"))

  private def push(client: MockSchemaRegistryClient): PushGenericRecord =
    new PushGenericRecord(srClient = client, serdeSettings = serdeSettings, topicName = TOPIC)

  private def wrapper(key_schema: Schema, value_schema: Schema, key: AnyRef, value: AnyRef): Record = {
    val record = new Record(NJConsumerRecord.schema(key_schema, value_schema))
    record.put("key", key)
    record.put("value", value)
    record
  }

  private def pull(
    client: MockSchemaRegistryClient,
    key_schema: Schema,
    value_schema: Schema): PullGenericRecord =
    new PullGenericRecord(
      AvroSchemaPair(new AvroSchema(key_schema), new AvroSchema(value_schema)),
      client)

  /** Round-trip a single key value through `PushGenericRecord` then `PullGenericRecord`, returning the
    * decoded `key` field. Confluent-framed bytes require a shared registry client across both sides.
    */
  private def roundTripKey(client: MockSchemaRegistryClient, key_schema: Schema, key: AnyRef): Any = {
    val produced = push(client)
      .fromGenericRecord(wrapper(key_schema, key_schema, key, key))
    pull(client, key_schema, key_schema)
      .toGenericRecord(fs2.kafka.ConsumerRecord(TOPIC.value, 0, 0L, produced.key, produced.value))
      .toOption
      .get
      .get("key")
  }

  test("1.STRING and INT values are Confluent-framed and round-trip") {
    val client = new MockSchemaRegistryClient
    val produced = push(client)
      .fromGenericRecord(wrapper(STRING_SCHEMA, INT_SCHEMA, "the-key", Integer.valueOf(42)))

    assertEquals(produced.topic, TOPIC.value)
    // Confluent wire format: magic byte 0x00 prefix
    assertEquals(produced.key.head, 0.toByte)
    assertEquals(produced.value.head, 0.toByte)

    val decoded = pull(client, STRING_SCHEMA, INT_SCHEMA)
      .toGenericRecord(fs2.kafka.ConsumerRecord(TOPIC.value, 0, 0L, produced.key, produced.value))
      .toOption
      .get
    assertEquals(decoded.get("key").toString, "the-key")
    assertEquals(decoded.get("value"), Integer.valueOf(42))
  }

  test("2.LONG, FLOAT, DOUBLE, and BOOLEAN round-trip through Confluent framing") {
    assertEquals(
      roundTripKey(new MockSchemaRegistryClient, LONG_SCHEMA, java.lang.Long.valueOf(42L)),
      java.lang.Long.valueOf(42L))
    assertEquals(
      roundTripKey(new MockSchemaRegistryClient, FLOAT_SCHEMA, java.lang.Float.valueOf(1.5f)),
      java.lang.Float.valueOf(1.5f))
    assertEquals(
      roundTripKey(new MockSchemaRegistryClient, DOUBLE_SCHEMA, java.lang.Double.valueOf(2.5d)),
      java.lang.Double.valueOf(2.5d))
    assertEquals(
      roundTripKey(new MockSchemaRegistryClient, BOOLEAN_SCHEMA, java.lang.Boolean.TRUE),
      java.lang.Boolean.TRUE)
  }

  test("3.RECORD key and value are Confluent-framed and register distinct subjects") {
    val client = new MockSchemaRegistryClient
    val key = new Record(RECORD_SCHEMA)
    key.put("name", "key")
    val value = new Record(RECORD_SCHEMA)
    value.put("name", "value")

    val produced = push(client)
      .fromGenericRecord(wrapper(RECORD_SCHEMA, RECORD_SCHEMA, key, value))

    assert(produced.key.length > 5)
    assert(produced.value.length > 5)
    assertEquals(produced.key.head, 0.toByte)
    assertEquals(produced.value.head, 0.toByte)
    assertEquals(client.getAllSubjects.asScala.toSet, Set(s"${TOPIC.value}-key", s"${TOPIC.value}-value"))
  }

  test("4.null values encode to null for every schema type") {
    val schemas = List(
      STRING_SCHEMA,
      BYTES_SCHEMA,
      INT_SCHEMA,
      LONG_SCHEMA,
      FLOAT_SCHEMA,
      DOUBLE_SCHEMA,
      BOOLEAN_SCHEMA,
      RECORD_SCHEMA)

    schemas.foreach { schema =>
      val produced = push(new MockSchemaRegistryClient)
        .fromGenericRecord(wrapper(schema, schema, null, null))
      assertEquals(produced.key, null)
      assertEquals(produced.value, null)
    }
  }
}
