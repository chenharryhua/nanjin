package com.github.chenharryhua.nanjin.kafka.serdes

import com.kjetland.jackson.jsonSchema.JsonSchemaDraft
import mtest.kafka.objectMapper
import org.scalatest.funsuite.AnyFunSuite

final case class JsonSchemaPerson(name: String, age: Int)

class KafkaJsonSchemaCodecTest extends AnyFunSuite {
  private val codec = KafkaJsonSchemaCodec[JsonSchemaPerson](objectMapper)
    .updateConfig(_.jsonSchemaDraft(JsonSchemaDraft.DRAFT_07))

  test("generates a JSON schema for the runtime class") {
    val schema = codec.schema
    val rendered = schema.toString

    assert(rendered.contains("name"))
    assert(rendered.contains("age"))
    assert(rendered.contains("type"))
  }

  test("wraps values in a Confluent JSON schema envelope and round-trips them") {
    val person = JsonSchemaPerson("alice", 30)

    val envelope = codec.from(person)

    assert(envelope.has("schema"))
    assert(envelope.has("payload"))
    assert(envelope.get("payload").get("name").asText() === "alice")
    assert(envelope.get("payload").get("age").asInt() === 30)

    val roundTripped = codec.to(envelope.get("payload"))
    assert(roundTripped === person)
  }
}
