package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.kafka.config.SerdeSettings
import com.github.chenharryhua.nanjin.kafka.record.NJConsumerRecord
import com.github.chenharryhua.nanjin.kafka.{AvroSchemaPair, TopicName}
import com.landoop.telecom.telecomitalia.telecommunications.Key
import fs2.kafka.{ConsumerRecord, Header, Headers}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.Record
import munit.FunSuite

import java.nio.ByteBuffer

class PullGenericRecordTest extends FunSuite {
  private val topic = "pull.generic.record.test"

  private def serdeSettings: SerdeSettings =
    SerdeSettings(
      Map(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "mock://pull-generic-record"))

  /** Confluent-frame a single value against `schema` using a shared registry client, so the produced bytes
    * can be decoded back by a `PullGenericRecord` built on the same client's schemas.
    */
  private def frame(client: MockSchemaRegistryClient, schema: Schema, value: AnyRef): Array[Byte] = {
    val push = new PushGenericRecord(client, serdeSettings, TopicName(topic))
    val envelope = new Record(NJConsumerRecord.schema(schema, schema))
    envelope.put("key", value)
    envelope.put("value", value)
    push.fromGenericRecord(envelope).key
  }

  test("1.primitive decoding: STRING key and INT value") {
    val keySchema = Schema.create(Schema.Type.STRING)
    val valSchema = Schema.create(Schema.Type.INT)
    val client = new MockSchemaRegistryClient

    val keyBytes = frame(client, keySchema, "the-key")
    val valBytes = frame(client, valSchema, Integer.valueOf(42))

    val pair = AvroSchemaPair(AvroSchema(keySchema), AvroSchema(valSchema))
    val pull = new PullGenericRecord(pair)

    val cr = ConsumerRecord(topic, 0, 1L, keyBytes, valBytes)
      .withHeaders(Headers.fromSeq(Seq(Header("h1", Array(1.toByte, 2.toByte)))))

    val res = pull.toGenericRecord(cr)
    assert(res.isRight)
    val record = res.toOption.get

    assert(record.get("topic").toString == topic)
    assert(record.get("partition") == 0)
    assert(record.get("offset") == 1L)
    assert(record.get("key").toString == "the-key")
    // avro record stores boxed java.lang.Integer for int
    assert(record.get("value") == Integer.valueOf(42))

    val headers = record.get("headers").asInstanceOf[java.util.List[Record]]
    assert(headers.size() == 1)
    val h = headers.get(0)
    assert(h.get("key") == "h1")
    val bb = h.get("value").asInstanceOf[java.nio.ByteBuffer]
    assert(bb.array().sameElements(Array(1.toByte, 2.toByte)))
  }

  test("2.null key and null value are preserved") {
    val keySchema = Schema.create(Schema.Type.STRING)
    val valSchema = Schema.create(Schema.Type.STRING)
    val pair = AvroSchemaPair(AvroSchema(keySchema), AvroSchema(valSchema))

    val pull = new PullGenericRecord(pair)

    val cr = ConsumerRecord(topic, 0, 2L, null.asInstanceOf[Array[Byte]], null.asInstanceOf[Array[Byte]])

    val res = pull.toGenericRecord(cr)
    assert(res.isRight)
    val record = res.toOption.get
    assert(record.get("key") == null)
    assert(record.get("value") == null)
  }

  test("3.payload shorter than 5 bytes produces PullError with isKey true") {
    val keyRecordSchema = new Schema.Parser().parse(Key.schema)
    val valSchema = Schema.create(Schema.Type.STRING)
    val client = new MockSchemaRegistryClient
    val pair = AvroSchemaPair(AvroSchema(keyRecordSchema), AvroSchema(valSchema))

    val pull = new PullGenericRecord(pair)

    // too short: fewer than the 5-byte Confluent prefix
    val badKeyBytes = Array(1.toByte, 2.toByte)
    val valBytes = frame(client, valSchema, "v")

    val cr = ConsumerRecord(topic, 1, 3L, badKeyBytes, valBytes)

    val res = pull.toGenericRecord(cr)
    assert(res.isLeft)
    val err = res.swap.toOption.get
    assert(err.isKey)
    assert(err.cause.getMessage.contains("too short"))
  }

  test("4.invalid magic byte produces PullError") {
    val keyRecordSchema = new Schema.Parser().parse(Key.schema)
    val valSchema = Schema.create(Schema.Type.STRING)
    val client = new MockSchemaRegistryClient
    val pair = AvroSchemaPair(AvroSchema(keyRecordSchema), AvroSchema(valSchema))

    val pull = new PullGenericRecord(pair)

    // 5+ bytes but wrong magic byte (0x01 instead of 0x00)
    val badMagic = Array[Byte](0x01, 0, 0, 0, 1, 0, 0)
    val valBytes = frame(client, valSchema, "v")

    val cr = ConsumerRecord(topic, 0, 4L, badMagic, valBytes)

    val res = pull.toGenericRecord(cr)
    assert(res.isLeft)
    val err = res.swap.toOption.get
    assert(err.isKey)
    assert(err.cause.getMessage.contains("magic byte"))
    assertEquals(err.toJson.asObject.map(_.keys.toSet), Some(Set("key.error")))
  }

  test("5.exactly 5 bytes with valid magic byte fails deeper in Avro decode, not wire-format validation") {
    val keySchema = Schema.create(Schema.Type.STRING)
    val valSchema = Schema.create(Schema.Type.STRING)
    val client = new MockSchemaRegistryClient
    val keySchemaId = client.register(s"$topic-key", AvroSchema(keySchema))
    val pair = AvroSchemaPair(AvroSchema(keySchema), AvroSchema(valSchema))

    val pull = new PullGenericRecord(pair)

    // Valid magic byte, 4-byte schema ID, but no payload — should fail during Avro decode, not wire format check
    val minimalWireFormat = ByteBuffer.allocate(5).put(0x00.toByte).putInt(keySchemaId).array()
    val valBytes = frame(client, valSchema, "v")

    val cr = ConsumerRecord(topic, 0, 5L, minimalWireFormat, valBytes)

    val res = pull.toGenericRecord(cr)
    assert(res.isLeft)
    val err = res.swap.toOption.get
    assert(err.isKey)
    // The error should NOT be about magic byte or payload length; it fails deeper, in Avro decode, whose
    // exception may carry a null message — treat null as "not a wire-format message".
    val message = Option(err.cause.getMessage)
    assert(message.forall(m => !m.contains("magic byte")))
    assert(message.forall(m => !m.contains("too short")))
  }

  test("6.value-side bad wire format produces PullError with isKey false") {
    val keySchema = Schema.create(Schema.Type.STRING)
    val valRecordSchema = new Schema.Parser().parse(Key.schema)
    val client = new MockSchemaRegistryClient
    val pair = AvroSchemaPair(AvroSchema(keySchema), AvroSchema(valRecordSchema))

    val pull = new PullGenericRecord(pair)

    val keyBytes = frame(client, keySchema, "k")
    val badValBytes = Array[Byte](0x01, 0, 0, 0, 1, 0) // wrong magic byte

    val cr = ConsumerRecord(topic, 0, 6L, keyBytes, badValBytes)

    val res = pull.toGenericRecord(cr)
    assert(res.isLeft)
    val err = res.swap.toOption.get
    assert(!err.isKey) // error is on value side
    assert(err.cause.getMessage.contains("magic byte"))
    assertEquals(err.toJson.asObject.map(_.keys.toSet), Some(Set("value.error")))
  }

  test("7.LONG, FLOAT, DOUBLE, BOOLEAN, and STRING round-trip through Confluent framing") {
    val long_schema = Schema.create(Schema.Type.LONG)
    val float_schema = Schema.create(Schema.Type.FLOAT)
    val double_schema = Schema.create(Schema.Type.DOUBLE)
    val boolean_schema = Schema.create(Schema.Type.BOOLEAN)
    val string_schema = Schema.create(Schema.Type.STRING)

    val client1 = new MockSchemaRegistryClient
    val long_float =
      new PullGenericRecord(AvroSchemaPair(AvroSchema(long_schema), AvroSchema(float_schema)))
        .toGenericRecord(
          ConsumerRecord(
            topic,
            0,
            7L,
            frame(client1, long_schema, java.lang.Long.valueOf(42L)),
            frame(client1, float_schema, java.lang.Float.valueOf(1.5f))
          ))
        .toOption
        .get

    assertEquals(long_float.get("key"), java.lang.Long.valueOf(42L))
    assertEquals(long_float.get("value"), java.lang.Float.valueOf(1.5f))

    val client2 = new MockSchemaRegistryClient
    val double_boolean =
      new PullGenericRecord(AvroSchemaPair(AvroSchema(double_schema), AvroSchema(boolean_schema)))
        .toGenericRecord(
          ConsumerRecord(
            topic,
            0,
            8L,
            frame(client2, double_schema, java.lang.Double.valueOf(2.5d)),
            frame(client2, boolean_schema, java.lang.Boolean.TRUE)
          ))
        .toOption
        .get

    assertEquals(double_boolean.get("key"), java.lang.Double.valueOf(2.5d))
    assertEquals(double_boolean.get("value"), java.lang.Boolean.TRUE)

    val client3 = new MockSchemaRegistryClient
    val boolean_string =
      new PullGenericRecord(AvroSchemaPair(AvroSchema(boolean_schema), AvroSchema(string_schema)))
        .toGenericRecord(
          ConsumerRecord(
            topic,
            0,
            9L,
            frame(client3, boolean_schema, java.lang.Boolean.TRUE),
            frame(client3, string_schema, "value")))
        .toOption
        .get

    assertEquals(boolean_string.get("key"), java.lang.Boolean.TRUE)
    assertEquals(boolean_string.get("value").toString, "value")
  }

  test("8.RECORD payload produced in Confluent wire format decodes successfully") {
    val record_schema =
      SchemaBuilder.record("Payload").namespace("mtest.kafka").fields().requiredString("name").endRecord()
    val string_schema = Schema.create(Schema.Type.STRING)
    val pair = AvroSchemaPair(AvroSchema(record_schema), AvroSchema(string_schema))
    val client = new MockSchemaRegistryClient
    val push = new PushGenericRecord(client, serdeSettings, TopicName(topic))

    val payload = new Record(record_schema)
    payload.put("name", "decoded")
    val envelope = new Record(pair.consumerSchema)
    envelope.put("key", payload)
    envelope.put("value", "value")
    val produced = push.fromGenericRecord(envelope)

    val decoded = new PullGenericRecord(pair)
      .toGenericRecord(ConsumerRecord(topic, 0, 10L, produced.key, produced.value))
      .toOption
      .get

    decoded.get("key") match {
      case record: Record => assertEquals(record.get("name").toString, "decoded")
      case other          => fail(s"expected GenericData.Record, got $other")
    }
    assertEquals(decoded.get("value").toString, "value")
  }

  test("9.null bytes decode to a null field without attempting Avro decoding") {
    val record_schema =
      SchemaBuilder.record("NullablePayload").namespace("mtest.kafka").fields().requiredString(
        "name").endRecord()
    val string_schema = Schema.create(Schema.Type.STRING)
    val client = new MockSchemaRegistryClient
    val pull =
      new PullGenericRecord(AvroSchemaPair(AvroSchema(record_schema), AvroSchema(string_schema)))
    val value_bytes = frame(client, string_schema, "value")

    val decoded = pull
      .toGenericRecord(ConsumerRecord(topic, 0, 11L, null: Array[Byte], value_bytes))
      .toOption
      .get

    assertEquals(decoded.get("key"), null)
    assertEquals(decoded.get("value").toString, "value")
  }

  test("10.a null header value decodes as empty bytes instead of throwing") {
    val string_schema = Schema.create(Schema.Type.STRING)
    val client = new MockSchemaRegistryClient
    val pull =
      new PullGenericRecord(AvroSchemaPair(AvroSchema(string_schema), AvroSchema(string_schema)))
    val bytes = frame(client, string_schema, "v")

    val cr = ConsumerRecord(topic, 0, 12L, bytes, bytes)
      .withHeaders(Headers.fromSeq(Seq(Header("h-null", null: Array[Byte]))))

    val decoded = pull.toGenericRecord(cr).toOption.get
    val headers = decoded.get("headers").asInstanceOf[java.util.List[Record]]

    assertEquals(headers.size(), 1)
    assertEquals(headers.get(0).get("key").toString, "h-null")
    assertEquals(headers.get(0).get("value").asInstanceOf[ByteBuffer].remaining(), 0)
  }

  test("11.PullError JSON carries the record metadata and the cause") {
    val string_schema = Schema.create(Schema.Type.STRING)
    val client = new MockSchemaRegistryClient
    val pull =
      new PullGenericRecord(AvroSchemaPair(AvroSchema(string_schema), AvroSchema(string_schema)))

    val err = pull
      .toGenericRecord(ConsumerRecord(topic, 3, 13L, Array[Byte](1, 2), frame(client, string_schema, "v")))
      .swap
      .toOption
      .get

    assert(err.isKey)
    val payload = err.toJson.asObject.flatMap(_.apply("key.error")).flatMap(_.asObject).get
    assertEquals(payload("partition").flatMap(_.asNumber).flatMap(_.toInt), Some(3))
    assertEquals(payload("offset").flatMap(_.asNumber).flatMap(_.toLong), Some(13L))

    val cause = payload("cause").flatMap(_.asString).get
    assert(cause.contains("IllegalArgumentException"), cause)
    assert(cause.contains("too short"), cause)
  }
}
