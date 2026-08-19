package com.github.chenharryhua.nanjin.kafka.connector

import com.github.chenharryhua.nanjin.kafka.AvroSchemaPair
import com.landoop.telecom.telecomitalia.telecommunications.Key
import fs2.kafka.{ConsumerRecord, Header, Headers}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.common.serialization.Serdes
import org.scalatest.funsuite.AnyFunSuite

class PullGenericRecordTest extends AnyFunSuite {
  private val topic = "pull.generic.record.test"

  test("1.primitive decoding: STRING key and INT value") {
    val keySchema = Schema.create(Schema.Type.STRING)
    val valSchema = Schema.create(Schema.Type.INT)
    val pair = AvroSchemaPair(AvroSchema(keySchema), AvroSchema(valSchema))

    val pull = new com.github.chenharryhua.nanjin.kafka.connector.PullGenericRecord(pair)

    val keyBytes: Array[Byte] = Serdes.String().serializer().serialize(topic, "the-key")
    val valBytes: Array[Byte] = Serdes.Integer().serializer().serialize(topic, Integer.valueOf(42))

    val cr = ConsumerRecord(topic, 0, 1L, keyBytes, valBytes)
      .withHeaders(Headers.fromSeq(Seq(Header("h1", Array(1.toByte, 2.toByte)))))

    val res = pull.toGenericRecord(cr)
    assert(res.isRight)
    val record = res.toOption.get

    assert(record.get("topic").toString == topic)
    assert(record.get("partition") == 0)
    assert(record.get("offset") == 1L)
    assert(record.get("key") == "the-key")
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

    val pull = new com.github.chenharryhua.nanjin.kafka.connector.PullGenericRecord(pair)

    val cr = ConsumerRecord(topic, 0, 2L, null.asInstanceOf[Array[Byte]], null.asInstanceOf[Array[Byte]])

    val res = pull.toGenericRecord(cr)
    assert(res.isRight)
    val record = res.toOption.get
    assert(record.get("key") == null)
    assert(record.get("value") == null)
  }

  test("3.record schema decode error produces PullError with isKey true") {
    // key is RECORD type: decoder expects Confluent wire format and will drop first 5 bytes
    val keyRecordSchema = new Schema.Parser().parse(Key.schema)
    // keyRecordSchema.setFields(java.util.Arrays.asList())

    val valSchema = Schema.create(Schema.Type.STRING)
    val pair = AvroSchemaPair(AvroSchema(keyRecordSchema), AvroSchema(valSchema))

    val pull = new com.github.chenharryhua.nanjin.kafka.connector.PullGenericRecord(pair)

    // supply too short byte array so data.drop(5) produces empty array and reader will fail
    val badKeyBytes = Array(1.toByte, 2.toByte)
    val valBytes: Array[Byte] = Serdes.String().serializer().serialize(topic, "v")

    val cr = ConsumerRecord(topic, 1, 3L, badKeyBytes, valBytes)

    val res = pull.toGenericRecord(cr)
    assert(res.isLeft)
    val err = res.swap.toOption.get
    assert(err.isKey)
  }

  test("4.record schema with invalid magic byte produces PullError") {
    val keyRecordSchema = new Schema.Parser().parse(Key.schema)
    val valSchema = Schema.create(Schema.Type.STRING)
    val pair = AvroSchemaPair(AvroSchema(keyRecordSchema), AvroSchema(valSchema))

    val pull = new com.github.chenharryhua.nanjin.kafka.connector.PullGenericRecord(pair)

    // 5+ bytes but wrong magic byte (0x01 instead of 0x00)
    val badMagic = Array[Byte](0x01, 0, 0, 0, 1, 0, 0)
    val valBytes: Array[Byte] = Serdes.String().serializer().serialize(topic, "v")

    val cr = ConsumerRecord(topic, 0, 4L, badMagic, valBytes)

    val res = pull.toGenericRecord(cr)
    assert(res.isLeft)
    val err = res.swap.toOption.get
    assert(err.isKey)
    assert(err.cause.getMessage.contains("magic byte"))
  }

  test("5.record schema with exactly 5 bytes and valid magic byte attempts decode") {
    val keyRecordSchema = new Schema.Parser().parse(Key.schema)
    val valSchema = Schema.create(Schema.Type.STRING)
    val pair = AvroSchemaPair(AvroSchema(keyRecordSchema), AvroSchema(valSchema))

    val pull = new com.github.chenharryhua.nanjin.kafka.connector.PullGenericRecord(pair)

    // Valid magic byte, 4-byte schema ID, but no payload — should fail during Avro decode, not wire format check
    val minimalWireFormat = Array[Byte](0x00, 0, 0, 0, 1)
    val valBytes: Array[Byte] = Serdes.String().serializer().serialize(topic, "v")

    val cr = ConsumerRecord(topic, 0, 5L, minimalWireFormat, valBytes)

    val res = pull.toGenericRecord(cr)
    // Should fail at Avro level (empty payload for a record schema), not at wire format validation
    assert(res.isLeft)
    val err = res.swap.toOption.get
    assert(err.isKey)
    // The error should NOT be about magic byte or payload length
    assert(!err.cause.getMessage.contains("magic byte"))
    assert(!err.cause.getMessage.contains("too short"))
  }

  test("6.value schema RECORD with bad wire format produces PullError with isKey false") {
    val keySchema = Schema.create(Schema.Type.STRING)
    val valRecordSchema = new Schema.Parser().parse(Key.schema)
    val pair = AvroSchemaPair(AvroSchema(keySchema), AvroSchema(valRecordSchema))

    val pull = new com.github.chenharryhua.nanjin.kafka.connector.PullGenericRecord(pair)

    val keyBytes: Array[Byte] = Serdes.String().serializer().serialize(topic, "k")
    val badValBytes = Array[Byte](0x01, 0, 0, 0, 1, 0) // wrong magic byte

    val cr = ConsumerRecord(topic, 0, 6L, keyBytes, badValBytes)

    val res = pull.toGenericRecord(cr)
    assert(res.isLeft)
    val err = res.swap.toOption.get
    assert(!err.isKey) // error is on value side
    assert(err.cause.getMessage.contains("magic byte"))
  }
}
