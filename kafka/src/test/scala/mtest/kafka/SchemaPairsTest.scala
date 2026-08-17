package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.*
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.apache.avro.SchemaBuilder
import org.scalatest.funsuite.AnyFunSuite

class SchemaPairsTest extends AnyFunSuite {

  // --- helper schemas ---

  private val avroKey = new AvroSchema(
    SchemaBuilder.record("Key").namespace("test").fields().requiredString("id").endRecord())
  private val avroValue = new AvroSchema(
    SchemaBuilder.record("Value").namespace("test").fields().requiredString("name").requiredInt("age").endRecord())

  // --- AvroSchemaPair ---

  test("1.AvroSchemaPair - consumerSchema is a record schema") {
    val pair = AvroSchemaPair(avroKey, avroValue)
    val cs = pair.consumerSchema
    assert(cs.getType == org.apache.avro.Schema.Type.RECORD)
    assert(cs.getFields.size() > 0)
  }

  test("2.AvroSchemaPair - consumerSchema includes key and value info") {
    val pair = AvroSchemaPair(avroKey, avroValue)
    val cs = pair.consumerSchema
    // NJConsumerRecord schema wraps key/value schemas
    assert(cs.toString.contains("key"))
    assert(cs.toString.contains("value"))
  }

  // --- OptionalJsonSchemaPair ---

  test("3.OptionalJsonSchemaPair - isBackwardCompatible when both None") {
    val local = OptionalJsonSchemaPair(None, None)
    val broker = OptionalJsonSchemaPair(None, None)
    assert(local.isBackwardCompatible(broker))
  }

  test("4.OptionalJsonSchemaPair - isBackwardCompatible when local None, broker Some") {
    val jsonKey = new JsonSchema("""{"type": "string"}""")
    val local = OptionalJsonSchemaPair(None, None)
    val broker = OptionalJsonSchemaPair(Some(jsonKey), None)
    assert(local.isBackwardCompatible(broker))
  }

  test("5.OptionalJsonSchemaPair - toSchemaPair throws when both absent") {
    val pair = OptionalJsonSchemaPair(None, None)
    val ex = intercept[TopicSchemaAbsent](pair.toSchemaPair)
    assert(ex.getMessage.contains("both key and value schema are absent"))
  }

  test("6.OptionalJsonSchemaPair - toSchemaPair throws when key absent") {
    val jsonValue = new JsonSchema("""{"type": "integer"}""")
    val pair = OptionalJsonSchemaPair(None, Some(jsonValue))
    val ex = intercept[TopicSchemaAbsent](pair.toSchemaPair)
    assert(ex.getMessage.contains("key schema is absent"))
  }

  test("7.OptionalJsonSchemaPair - toSchemaPair throws when value absent") {
    val jsonKey = new JsonSchema("""{"type": "string"}""")
    val pair = OptionalJsonSchemaPair(Some(jsonKey), None)
    val ex = intercept[TopicSchemaAbsent](pair.toSchemaPair)
    assert(ex.getMessage.contains("value schema is absent"))
  }

  test("8.OptionalJsonSchemaPair - toSchemaPair succeeds when both present") {
    val jsonKey = new JsonSchema("""{"type": "string"}""")
    val jsonValue = new JsonSchema("""{"type": "object", "properties": {"a": {"type": "integer"}}}""")
    val pair = OptionalJsonSchemaPair(Some(jsonKey), Some(jsonValue))
    val result = pair.toSchemaPair
    assert(result.key == jsonKey)
    assert(result.value == jsonValue)
  }

  test("9.OptionalJsonSchemaPair - read prefers local, falls back to broker") {
    val jsonKey = new JsonSchema("""{"type": "string"}""")
    val jsonValue = new JsonSchema("""{"type": "integer"}""")
    val local = OptionalJsonSchemaPair(Some(jsonKey), None)
    val broker = OptionalJsonSchemaPair(None, Some(jsonValue))

    val result = local.read(broker)
    assert(result.key.contains(jsonKey))
    assert(result.value.contains(jsonValue))
  }

  test("10.OptionalJsonSchemaPair - write prefers broker, falls back to local") {
    val jsonKey = new JsonSchema("""{"type": "string"}""")
    val jsonValue = new JsonSchema("""{"type": "integer"}""")
    val local = OptionalJsonSchemaPair(Some(jsonKey), None)
    val broker = OptionalJsonSchemaPair(None, Some(jsonValue))

    val result = local.write(broker)
    assert(result.key.contains(jsonKey)) // broker None, fallback to local
    assert(result.value.contains(jsonValue)) // broker wins
  }

  // --- OptionalProtobufSchemaPair ---

  test("11.OptionalProtobufSchemaPair - toSchemaPair throws when key absent") {
    val protoValue = new ProtobufSchema("syntax = \"proto3\"; message Value { string name = 1; }")
    val pair = OptionalProtobufSchemaPair(None, Some(protoValue))
    val ex = intercept[TopicSchemaAbsent](pair.toSchemaPair)
    assert(ex.getMessage.contains("key schema is absent"))
  }

  test("12.OptionalProtobufSchemaPair - toSchemaPair succeeds when both present") {
    val protoKey = new ProtobufSchema("syntax = \"proto3\"; message Key { string id = 1; }")
    val protoValue = new ProtobufSchema("syntax = \"proto3\"; message Value { string name = 1; }")
    val pair = OptionalProtobufSchemaPair(Some(protoKey), Some(protoValue))
    val result = pair.toSchemaPair
    assert(result.key == protoKey)
    assert(result.value == protoValue)
  }

  test("13.OptionalProtobufSchemaPair - read/write semantics") {
    val protoKey = new ProtobufSchema("syntax = \"proto3\"; message Key { string id = 1; }")
    val protoValue = new ProtobufSchema("syntax = \"proto3\"; message Value { string name = 1; }")
    val local = OptionalProtobufSchemaPair(Some(protoKey), None)
    val broker = OptionalProtobufSchemaPair(None, Some(protoValue))

    val read = local.read(broker)
    assert(read.key.contains(protoKey))
    assert(read.value.contains(protoValue))

    val write = local.write(broker)
    assert(write.key.contains(protoKey))
    assert(write.value.contains(protoValue))
  }

  // --- Exceptions ---

  test("14.SchemaRegistryUrlAbsent - has meaningful message") {
    val ex = SchemaRegistryUrlAbsent("schema.registry.url")
    assert(ex.getMessage.contains("schema.registry.url"))
    assert(ex.getMessage.contains("absent"))
    assert(ex.isInstanceOf[IllegalStateException])
  }

  test("15.TopicSchemaAbsent - has meaningful message and no stack trace") {
    val ex = TopicSchemaAbsent("key schema missing")
    assert(ex.getMessage.contains("key schema missing"))
    assert(ex.getStackTrace.isEmpty)
  }

  test("16.SchemaIncompatible - includes topic name") {
    val ex = SchemaIncompatible(TopicName("orders"))
    assert(ex.getMessage.contains("orders"))
    assert(ex.getMessage.toLowerCase.contains("incompatible"))
    assert(ex.getStackTrace.isEmpty)
  }

  test("17.EmptyTopicPartitionMap - includes topic name") {
    val ex = EmptyTopicPartitionMap(TopicName("events"))
    assert(ex.getMessage.contains("events"))
    assert(ex.getStackTrace.isEmpty)
  }
}
