package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.kafka.config.SerdeSettings
import com.github.chenharryhua.nanjin.kafka.record.NJConsumerRecord
import com.github.chenharryhua.nanjin.kafka.{AvroSchemaPair, TopicName}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.common.serialization.Serdes
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

  // Primitive encoders do not touch the schema registry; a mock client suffices.
  private def push(key_schema: Schema, value_schema: Schema): PushGenericRecord =
    push(new MockSchemaRegistryClient, key_schema, value_schema)

  private def push(
    client: MockSchemaRegistryClient,
    key_schema: Schema,
    value_schema: Schema): PushGenericRecord =
    new PushGenericRecord(
      srClient = client,
      serdeSettings = SerdeSettings(
        Map(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "mock://push-generic-record")),
      topicName = TOPIC,
      pair = AvroSchemaPair(new AvroSchema(key_schema), new AvroSchema(value_schema))
    )

  private def wrapper(key_schema: Schema, value_schema: Schema, key: AnyRef, value: AnyRef): Record = {
    val record = new Record(NJConsumerRecord.schema(key_schema, value_schema))
    record.put("key", key)
    record.put("value", value)
    record
  }

  private def primitive_record(key_schema: Schema, key: AnyRef) =
    push(key_schema, INT_SCHEMA).fromGenericRecord(wrapper(key_schema, INT_SCHEMA, key, Integer.valueOf(1)))

  test("1.STRING and INT values round-trip through their Kafka Serdes") {
    val produced = push(STRING_SCHEMA, INT_SCHEMA)
      .fromGenericRecord(wrapper(STRING_SCHEMA, INT_SCHEMA, "the-key", Integer.valueOf(42)))

    assertEquals(produced.topic, TOPIC.value)
    assertEquals(Serdes.String().deserializer().deserialize(TOPIC.value, produced.key), "the-key")
    assertEquals(
      Serdes.Integer().deserializer().deserialize(TOPIC.value, produced.value),
      Integer.valueOf(42))
  }

  test("2.BYTES, LONG, FLOAT, DOUBLE, and BOOLEAN use their matching Kafka Serdes") {
    val bytes = Array[Byte](1, 2, 3)
    val bytes_record = primitive_record(BYTES_SCHEMA, bytes)
    val long_record = primitive_record(LONG_SCHEMA, java.lang.Long.valueOf(42L))
    val float_record = primitive_record(FLOAT_SCHEMA, java.lang.Float.valueOf(1.5f))
    val double_record = primitive_record(DOUBLE_SCHEMA, java.lang.Double.valueOf(2.5d))
    val boolean_record = primitive_record(BOOLEAN_SCHEMA, java.lang.Boolean.TRUE)

    assert(Serdes.ByteArray().deserializer().deserialize(TOPIC.value, bytes_record.key).sameElements(bytes))
    assertEquals(
      Serdes.Long().deserializer().deserialize(TOPIC.value, long_record.key),
      java.lang.Long.valueOf(42L))
    assertEquals(
      Serdes.Float().deserializer().deserialize(TOPIC.value, float_record.key),
      java.lang.Float.valueOf(1.5f))
    assertEquals(
      Serdes.Double().deserializer().deserialize(TOPIC.value, double_record.key),
      java.lang.Double.valueOf(2.5d))
    assertEquals(
      Serdes.Boolean().deserializer().deserialize(TOPIC.value, boolean_record.key),
      java.lang.Boolean.TRUE)
  }

  test("3.RECORD key and value use Confluent wire format and register distinct subjects") {
    val client = new MockSchemaRegistryClient
    val key = new Record(RECORD_SCHEMA)
    key.put("name", "key")
    val value = new Record(RECORD_SCHEMA)
    value.put("name", "value")

    val produced = push(client, RECORD_SCHEMA, RECORD_SCHEMA)
      .fromGenericRecord(wrapper(RECORD_SCHEMA, RECORD_SCHEMA, key, value))

    assert(produced.key.length > 5)
    assert(produced.value.length > 5)
    assertEquals(produced.key.head, 0.toByte)
    assertEquals(produced.value.head, 0.toByte)
    assertEquals(client.getAllSubjects.asScala.toSet, Set(s"${TOPIC.value}-key", s"${TOPIC.value}-value"))
  }

  test("4.null values encode to null for every supported schema type") {
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
      val produced = push(schema, schema).fromGenericRecord(wrapper(schema, schema, null, null))
      assertEquals(produced.key, null)
      assertEquals(produced.value, null)
    }
  }

  test("5.each supported schema rejects a mismatched runtime value") {
    val mismatches: List[(Schema, AnyRef)] = List(
      STRING_SCHEMA -> Integer.valueOf(1),
      BYTES_SCHEMA -> "not-bytes",
      INT_SCHEMA -> "not-int",
      LONG_SCHEMA -> "not-long",
      FLOAT_SCHEMA -> "not-float",
      DOUBLE_SCHEMA -> "not-double",
      BOOLEAN_SCHEMA -> "not-boolean",
      RECORD_SCHEMA -> "not-a-record"
    )

    mismatches.foreach { case (schema, invalid) =>
      val error = intercept[IllegalArgumentException] {
        primitive_record(schema, invalid)
      }
      assert(error.getMessage.startsWith(invalid.getClass.getName))
    }
  }

  test("6.unsupported schema type raises UnsupportedOperationException with the schema type") {
    val array_schema = Schema.createArray(INT_SCHEMA)
    val error = intercept[UnsupportedOperationException] {
      push(array_schema, INT_SCHEMA)
    }
    assert(error.getMessage.contains("ARRAY"))
  }

  test("7.RECORD schema migration failures propagate the underlying Avro error") {
    val incompatible_schema =
      SchemaBuilder.record("Incompatible").namespace("mtest.kafka").fields().requiredInt("other").endRecord()
    val incompatible = new Record(incompatible_schema)
    incompatible.put("other", Integer.valueOf(1))

    intercept[org.apache.avro.AvroTypeException] {
      push(RECORD_SCHEMA, INT_SCHEMA)
        .fromGenericRecord(wrapper(RECORD_SCHEMA, INT_SCHEMA, incompatible, Integer.valueOf(1)))
    }
  }
}
