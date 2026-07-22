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

  test("primitive decoding: STRING key and INT value") {
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

  test("null key and null value are preserved") {
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

  test("record schema decode error produces PullError with isKey true") {
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
}
