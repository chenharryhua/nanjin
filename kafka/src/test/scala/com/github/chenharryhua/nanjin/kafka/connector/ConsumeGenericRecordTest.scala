package com.github.chenharryhua.nanjin.kafka.connector

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.{OptionalAvroSchemaPair, TopicName}
import fs2.kafka.{ConsumerSettings, Deserializer}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite

class ConsumeGenericRecordTest extends AnyFunSuite {

  private val topic: TopicName = TopicName("consume.generic.record.test")

  private val stringSchema: Schema = Schema.create(Schema.Type.STRING)
  private val intSchema: Schema = Schema.create(Schema.Type.INT)

  private def baseSettings: ConsumerSettings[IO, Array[Byte], Array[Byte]] =
    ConsumerSettings[IO, Array[Byte], Array[Byte]](
      Deserializer[IO, Array[Byte]],
      Deserializer[IO, Array[Byte]])

  private def consumer(
    caller: OptionalAvroSchemaPair,
    broker: OptionalAvroSchemaPair): ConsumeGenericRecord[IO] =
    new ConsumeGenericRecord[IO](
      topicName = topic,
      schemaPair = caller,
      fromSchemaRegistry = IO.pure(broker),
      consumerSettings = baseSettings)

  test("1.updateConfig transforms the underlying consumer settings") {
    val c = consumer(
      OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), Some(AvroSchema(intSchema))),
      OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), Some(AvroSchema(intSchema)))
    ).updateConfig(_.withGroupId("nanjin-test"))

    assert(c.properties.get("group.id").contains("nanjin-test"))
  }

  test("2.updateConfig returns a distinct instance, leaving the original unchanged") {
    val original = consumer(
      OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), Some(AvroSchema(intSchema))),
      OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), Some(AvroSchema(intSchema)))
    )
    val updated = original.updateConfig(_.withGroupId("group-x"))

    assert(!original.properties.contains("group.id"))
    assert(updated.properties.get("group.id").contains("group-x"))
    assert(original ne updated)
  }

  test("3.schema resolves the effective consumer schema from the registry") {
    // caller supplies nothing; the broker (registry) provides both schemas
    val c = consumer(
      OptionalAvroSchemaPair(None, None),
      OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), Some(AvroSchema(intSchema)))
    )
    val resolved = c.schema.unsafeRunSync()
    // the consumer schema is the wrapper record; its value field carries the resolved value schema
    assert(resolved.getType === Schema.Type.RECORD)
  }

  test("4.caller-supplied schema takes precedence over the broker schema") {
    // caller value schema is STRING, broker value schema is INT; read() prefers the caller's
    val callerValue = Schema.create(Schema.Type.STRING)
    val brokerValue = Schema.create(Schema.Type.INT)
    val c = consumer(
      OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), Some(AvroSchema(callerValue))),
      OptionalAvroSchemaPair(Some(AvroSchema(stringSchema)), Some(AvroSchema(brokerValue)))
    )
    val resolved = c.schema.unsafeRunSync()
    val valueField = resolved.getField("value")
    assert(valueField != null)
    // the value field is a nullable union; it should carry the caller's STRING, not the broker's INT
    val memberTypes: Set[Schema.Type] =
      import scala.jdk.CollectionConverters.*
      valueField.schema().getType match {
        case Schema.Type.UNION => valueField.schema().getTypes.asScala.map(_.getType).toSet
        case other             => Set(other)
      }
    assert(memberTypes.contains(Schema.Type.STRING))
    assert(!memberTypes.contains(Schema.Type.INT))
  }
}
