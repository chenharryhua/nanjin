package com.github.chenharryhua.nanjin.kafka.serdes

import com.fasterxml.jackson.databind.JsonNode
import mtest.kafka.objectMapper
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite

final case class JsonPerson(name: String, age: Int)
final case class AvroPerson(name: String, age: Int)

class BiTransformTest extends AnyFunSuite {
  test("integer option conversion preserves values and nulls") {
    val bi = summon[BiTransform[java.lang.Integer, Option[Int]]]

    assert(bi.to(Integer.valueOf(42)) === Some(42))
    assert(bi.from(Some(42)) === Integer.valueOf(42))
    assert(bi.from(None) === null)
  }

  test("generic record transform round-trips to and from a case class") {
    val bi = summon[BiTransform[GenericRecord, AvroPerson]]
    val person = AvroPerson("bob", 40)

    val record: GenericRecord = bi.from(person)
    assert(record.get("name").toString === "bob")
    assert(record.get("age") === 40)
    assert(bi.to(record).name === person.name)
    assert(bi.to(record).age === person.age)
  }

  test("JSON node transform round-trips to and from a case class") {
    given mapper: objectMapper.type = objectMapper
    val bi = summon[BiTransform[JsonNode, JsonPerson]]
    val person = JsonPerson("bob", 40)

    val envelope: JsonNode = bi.from(person)
    assert(envelope.has("schema"))
    assert(envelope.has("payload"))
    assert(envelope.get("payload").get("name").asText() === person.name)
    assert(envelope.get("payload").get("age").asInt() === person.age)
    assert(bi.to(envelope.get("payload")) === person)
  }
}
