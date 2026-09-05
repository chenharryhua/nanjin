package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.kafka.config.SerdeSettings
import com.github.chenharryhua.nanjin.kafka.record.NJConsumerRecord
import com.github.chenharryhua.nanjin.kafka.{AvroSchemaPair, TopicName}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.common.serialization.Serdes
import org.scalatest.funsuite.AnyFunSuite

class PushGenericRecordTest extends AnyFunSuite {

  private val topic: TopicName = TopicName("push.generic.record.unit.test")

  private val stringSchema: Schema = Schema.create(Schema.Type.STRING)
  private val intSchema: Schema = Schema.create(Schema.Type.INT)

  // primitive encoders do not touch the schema registry; a mock client suffices
  private def push(key: Schema, value: Schema): PushGenericRecord =
    new PushGenericRecord(
      srClient = new MockSchemaRegistryClient,
      serdeSettings = SerdeSettings(Map.empty),
      topicName = topic,
      pair = AvroSchemaPair(new AvroSchema(key), new AvroSchema(value))
    )

  // build an NJConsumerRecord-shaped wrapper carrying only key/value
  private def wrapper(key: AnyRef, value: AnyRef): Record = {
    val schema = NJConsumerRecord.schema(stringSchema, intSchema)
    val r = new Record(schema)
    r.put("key", key)
    r.put("value", value)
    r
  }

  test("1.primitive key/value round-trip through Serdes") {
    val pr = push(stringSchema, intSchema).fromGenericRecord(wrapper("the-key", Integer.valueOf(42)))
    assert(pr.topic === topic.value)
    // decode the produced bytes back with the matching Kafka deserializers
    assert(Serdes.String().deserializer().deserialize(topic.value, pr.key) === "the-key")
    assert(Serdes.Integer().deserializer().deserialize(topic.value, pr.value) === 42)
  }

  test("2.null key and value encode to null bytes") {
    val pr = push(stringSchema, intSchema).fromGenericRecord(wrapper(null, null))
    assert(pr.key == null)
    assert(pr.value == null)
  }

  test("3.type mismatch raises IllegalArgumentException") {
    // value schema is INT but we supply a String
    assertThrows[IllegalArgumentException] {
      push(stringSchema, intSchema).fromGenericRecord(wrapper("k", "not-an-int"))
    }
  }

  test("4.unsupported schema type raises UnsupportedOperationException") {
    // an ARRAY schema is not a supported key/value type
    val arraySchema = Schema.createArray(Schema.create(Schema.Type.INT))
    assertThrows[UnsupportedOperationException] {
      push(arraySchema, intSchema)
    }
  }
}
