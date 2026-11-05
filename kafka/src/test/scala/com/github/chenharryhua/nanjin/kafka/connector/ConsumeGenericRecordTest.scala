package com.github.chenharryhua.nanjin.kafka.connector

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{OptionalAvroSchemaPair, TopicName}
import com.github.chenharryhua.nanjin.kafka.TopicSchemaAbsent
import fs2.kafka.{ConsumerSettings, Deserializer}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import munit.CatsEffectSuite

class ConsumeGenericRecordTest extends CatsEffectSuite {

  private val topic: TopicName = TopicName("consume.generic.record.test")

  private val stringSchema: Schema = Schema.create(Schema.Type.STRING)
  private val intSchema: Schema = Schema.create(Schema.Type.INT)

  private def baseSettings: ConsumerSettings[IO, Array[Byte], Array[Byte]] =
    ConsumerSettings[IO, Array[Byte], Array[Byte]](
      Deserializer[IO, Array[Byte]],
      Deserializer[IO, Array[Byte]])

  private def consumer(broker: OptionalAvroSchemaPair): ConsumeGenericRecord[IO] =
    new ConsumeGenericRecord[IO](
      topicName = topic,
      fromSchemaRegistry = IO.pure(broker),
      consumerSettings = baseSettings)

  private val bothSides: OptionalAvroSchemaPair =
    OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), Some(AvroSchema(intSchema)))

  test("1.updateConfig transforms the underlying consumer settings") {
    val c = consumer(bothSides).updateConfig(_.withGroupId("nanjin-test"))

    assert(c.properties.get("group.id").contains("nanjin-test"))
  }

  test("2.updateConfig returns a distinct instance, leaving the original unchanged") {
    val original = consumer(bothSides)
    val updated = original.updateConfig(_.withGroupId("group-x"))

    assert(!original.properties.contains("group.id"))
    assert(updated.properties.get("group.id").contains("group-x"))
    assert(original ne updated)
  }

  test("3.schema resolves the effective consumer schema from the registry") {
    val c = consumer(bothSides)
    c.schema.map { resolved =>
      // the consumer schema is the wrapper record; its value field carries the resolved value schema
      assert(resolved.getType == Schema.Type.RECORD)
    }
  }

  test("4.schema fails when the registry is missing a side") {
    // the value subject is absent from the registry; toSchemaPair cannot build a pair
    val c = consumer(OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), None))
    c.schema.attempt.map { result =>
      assert(result.isLeft)
      assert(result.swap.toOption.exists(_.isInstanceOf[TopicSchemaAbsent]))
    }
  }
}
