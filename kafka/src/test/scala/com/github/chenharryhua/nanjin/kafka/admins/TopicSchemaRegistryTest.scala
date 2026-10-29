package com.github.chenharryhua.nanjin.kafka.admins

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{RegisteredSchemaId, TopicName}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.apache.avro.SchemaBuilder
import munit.CatsEffectSuite

/** Offline coverage for `TopicSchemaRegistry`.
  *
  * Backed by Confluent's in-memory `MockSchemaRegistryClient` (already a compile dependency), so register /
  * fetch / delete round-trips run without a Schema Registry server. Each test gets a fresh client, so the
  * registered subjects do not leak between tests.
  *
  * The suite pins the wrapper's own behavior on top of the client: the `<topic>-key` / `<topic>-value`
  * subject naming, the `fetchOptional*` schema-type filter (a side registered as a different type reads back
  * as `None`), `fetchAvroSchema` requiring both sides, and `delete` tolerating absent subjects.
  */
class TopicSchemaRegistryTest extends CatsEffectSuite {

  private val topic: TopicName = TopicName("topic.schema.registry.test")

  private def registry: TopicSchemaRegistry[IO] =
    TopicSchemaRegistry[IO](new MockSchemaRegistryClient(), topic)

  private val avroKey: AvroSchema = new AvroSchema(
    SchemaBuilder.record("Key").namespace("test").fields().requiredString("id").endRecord())
  private val avroValue: AvroSchema = new AvroSchema(
    SchemaBuilder.record("Value").namespace("test").fields().requiredString("name").endRecord())

  private val jsonKey: JsonSchema = new JsonSchema("""{"type": "string"}""")
  private val protoKey: ProtobufSchema =
    new ProtobufSchema("syntax = \"proto3\"; message Key { string id = 1; }")
  private val protoValue: ProtobufSchema =
    new ProtobufSchema("syntax = \"proto3\"; message Value { string name = 1; }")

  test("1.register key and value returns both assigned ids") {
    val sr = registry
    sr.register(Some(avroKey), Some(avroValue)).map { ids =>
      assert(ids.key.isDefined, s"expected a key id, got $ids")
      assert(ids.value.isDefined, s"expected a value id, got $ids")
    }
  }

  test("2.register only the key leaves the value id empty") {
    registry.register(key = Some(avroKey)).map { ids =>
      assert(ids.key.isDefined)
      assertEquals(ids.value, None)
    }
  }

  test("3.register with neither side registers nothing") {
    registry.register().map { ids =>
      assertEquals(ids, RegisteredSchemaId(None, None))
    }
  }

  test("4.fetchAvroSchema returns both sides once registered") {
    val sr = registry
    for {
      _ <- sr.register(Some(avroKey), Some(avroValue))
      fetched <- sr.fetchAvroSchema
    } yield {
      val (k, v) = fetched
      assertEquals(k.rawSchema(), avroKey.rawSchema())
      assertEquals(v.rawSchema(), avroValue.rawSchema())
    }
  }

  test("5.fetchAvroSchema fails when a side is missing") {
    val sr = registry
    // only the key is registered; fetching the (absent) value subject raises
    for {
      _ <- sr.register(key = Some(avroKey))
      res <- sr.fetchAvroSchema.attempt
    } yield assert(res.isLeft, "expected fetchAvroSchema to fail when the value subject is absent")
  }

  test("6.fetchOptionalAvroSchema is Some for a registered side and None for an absent side") {
    val sr = registry
    for {
      _ <- sr.register(key = Some(avroKey))
      pair <- sr.fetchOptionalAvroSchema
    } yield {
      assertEquals(pair.key.map(_.rawSchema()), Some(avroKey.rawSchema()))
      assertEquals(pair.value, None)
    }
  }

  test("7.fetchOptionalAvroSchema ignores a side registered as a non-Avro schema") {
    val sr = registry
    // the key subject holds a JSON schema -> the AVRO fetch filters it out
    for {
      _ <- sr.register(key = Some(jsonKey), value = Some(avroValue))
      pair <- sr.fetchOptionalAvroSchema
    } yield {
      assertEquals(pair.key, None)
      assertEquals(pair.value.map(_.rawSchema()), Some(avroValue.rawSchema()))
    }
  }

  test("8.fetchOptionalJsonSchema returns only JSON-typed subjects") {
    val sr = registry
    for {
      _ <- sr.register(key = Some(jsonKey), value = Some(avroValue))
      pair <- sr.fetchOptionalJsonSchema
    } yield {
      assert(pair.key.isDefined, "the JSON key schema should be returned")
      assertEquals(pair.value, None) // the value is Avro, filtered out
    }
  }

  test("9.fetchOptionalProtobufSchema returns both Protobuf-typed subjects") {
    val sr = registry
    for {
      _ <- sr.register(key = Some(protoKey), value = Some(protoValue))
      pair <- sr.fetchOptionalProtobufSchema
    } yield {
      assert(pair.key.isDefined, "the Protobuf key schema should be returned")
      assert(pair.value.isDefined, "the Protobuf value schema should be returned")
    }
  }

  test("10.delete removes both subjects and returns the deleted id lists") {
    val sr = registry
    for {
      _ <- sr.register(Some(avroKey), Some(avroValue))
      deleted <- sr.delete
      // after deletion, fetching requires both subjects and must fail
      afterKey <- sr.fetchOptionalAvroSchema
    } yield {
      val (k, v) = deleted
      assert(k.nonEmpty, s"expected deleted key ids, got $k")
      assert(v.nonEmpty, s"expected deleted value ids, got $v")
      assertEquals(afterKey, com.github.chenharryhua.nanjin.kafka.OptionalAvroSchemaPair(None, None))
    }
  }

  test("11.delete of absent subjects yields empty lists rather than failing") {
    registry.delete.map { case (k, v) =>
      assertEquals(k, List.empty[Integer])
      assertEquals(v, List.empty[Integer])
    }
  }
}
