package mtest.kafka

import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.serdes.Structured
import io.circe.Json
import io.circe.jawn.parse
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serde
import munit.FunSuite

class JsonStructuredSerdeSpec extends FunSuite {

  val serde: Serde[Json] = ctx.asValue(Structured[Json]).serde

  test("1.serializer should serialize Json to UTF-8 bytes") {
    val json = parse("""{"name":"test","value":123}""").toOption.get

    val bytes = serde.serializer.serialize("topic", json)

    assertEquals(new String(bytes, "UTF-8"), """{"name":"test","value":123}""")
  }

  test("2.serializer should return null for null input") {
    assertEquals(serde.serializer.serialize("topic", null), null)
  }

  test("3.deserializer should deserialize valid JSON bytes") {
    val jsonString = """{"name":"test","value":123}"""
    val bytes = jsonString.getBytes("UTF-8")

    val result = serde.deserializer.deserialize("topic", bytes)

    assertEquals(result, parse(jsonString).toOption.get)
  }

  test("4.deserializer should return null for null input") {
    assertEquals(serde.deserializer.deserialize("topic", null), null)
  }

  test("5.deserializer should throw SerializationException for invalid JSON") {
    val invalidJson = """{"name":"test","value":}"""
    val bytes = invalidJson.getBytes("UTF-8")

    val ex = intercept[SerializationException] {
      serde.deserializer.deserialize("topic", bytes)
    }

    assert(ex.getCause != null)
  }

  test("6.serializer should correctly serialize Json.Null") {
    val bytes = serde.serializer.serialize("topic", Json.Null)

    assertEquals(new String(bytes, "UTF-8"), "null")
  }

  test("7.deserializer should correctly deserialize JSON null") {
    val bytes = "null".getBytes("UTF-8")

    val result = serde.deserializer.deserialize("topic", bytes)

    assertEquals(result, Json.Null)
  }

  test("8.jsonSchema key deserializer configures with JSON_KEY_TYPE and builds offline") {
    // asKey forces the deserializer, whose lazy configure runs with isKey = true,
    // exercising the JSON_KEY_TYPE arm of the jsonSchema serde's configure.
    val keySerde: Serde[JsonNode] = ctx.asKey(Structured[JsonNode]).serde
    assert(keySerde.deserializer != null)
  }

}
