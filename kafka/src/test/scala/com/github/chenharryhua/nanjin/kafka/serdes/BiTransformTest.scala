package com.github.chenharryhua.nanjin.kafka.serdes

import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.record.ProtoConsumerRecord.ProtoConsumerRecord
import com.google.protobuf.DynamicMessage
import io.circe.Json
import io.scalaland.chimney.Iso as ChimneyIso
import monocle.Iso as MonocleIso
import mtest.kafka.objectMapper
import org.apache.avro.generic.GenericRecord
import munit.FunSuite

final case class JsonPerson(name: String, age: Int)
final case class AvroPerson(name: String, age: Int)

class BiTransformTest extends FunSuite {
  private def round_trip[A, B](codec: KafkaCodec[A, B], value: B): B =
    codec.to(codec.from(value))

  test("1.integer option conversion preserves values and nulls") {
    val bi = summon[BiTransform[java.lang.Integer, Option[Int]]]

    assert(bi.to(Integer.valueOf(42)) == Some(42))
    assert(bi.from(Some(42)) == Integer.valueOf(42))
    assert(bi.from(None) == null)
  }

  test("2.generic record transform round-trips to and from a case class") {
    val bi = summon[BiTransform[GenericRecord, AvroPerson]]
    val person = AvroPerson("bob", 40)

    val record: GenericRecord = bi.from(person)
    assert(record.get("name").toString == "bob")
    assert(record.get("age") == 40)
    assert(bi.to(record).name == person.name)
    assert(bi.to(record).age == person.age)
  }

  test("3.JSON node transform round-trips to and from a case class") {
    given mapper: objectMapper.type = objectMapper
    val bi = summon[BiTransform[JsonNode, JsonPerson]]
    val person = JsonPerson("bob", 40)

    val envelope: JsonNode = bi.from(person)
    assert(envelope.has("schema"))
    assert(envelope.has("payload"))
    assert(envelope.get("payload").get("name").asText() == person.name)
    assert(envelope.get("payload").get("age").asInt() == person.age)
    assert(bi.to(envelope.get("payload")) == person)
  }

  test("4.generic Kafka codecs hide different concrete schema types and round-trip values") {
    val avro_codec: KafkaCodec[GenericRecord, AvroPerson] = KafkaCodec.avro[AvroPerson]
    val protobuf_codec: KafkaCodec[DynamicMessage, ProtoConsumerRecord] =
      KafkaCodec.protobuf[ProtoConsumerRecord]
    val avro_person = AvroPerson("bob", 40)
    val proto_record = ProtoConsumerRecord("abc")

    assert(round_trip(avro_codec, avro_person) == avro_person)
    assert(round_trip(protobuf_codec, proto_record) == proto_record)
  }

  test("5.Json BiTransform.to throws when the JSON does not decode to the target type") {
    val bi = summon[BiTransform[Json, Int]]
    // a JSON string cannot decode as an Int, so the decode-failure arm throws
    intercept[io.circe.DecodingFailure](bi.to(Json.fromString("not an int")))
  }

  test("6.chimney Iso derives a BiTransform that round-trips in both directions") {
    given ChimneyIso[String, List[Char]] =
      ChimneyIso(
        first = (src: String) => src.toList,
        second = (src: List[Char]) => src.mkString
      )
    val bi = summon[BiTransform[String, List[Char]]]

    assertEquals(bi.to("abc"), List('a', 'b', 'c'))
    assertEquals(bi.from(List('x', 'y')), "xy")
  }

  test("7.monocle Iso derives a BiTransform that round-trips in both directions") {
    given MonocleIso[String, List[Char]] =
      MonocleIso[String, List[Char]](_.toList)(_.mkString)
    val bi = summon[BiTransform[String, List[Char]]]

    assertEquals(bi.to("abc"), List('a', 'b', 'c'))
    assertEquals(bi.from(List('x', 'y')), "xy")
  }
}
