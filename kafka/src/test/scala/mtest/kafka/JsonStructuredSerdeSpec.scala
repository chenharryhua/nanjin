package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.serdes.Structured
import io.circe.Json
import io.circe.jawn.parse
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serde
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JsonStructuredSerdeSpec extends AnyFunSuite with Matchers {

  val serde: Serde[Json] = ctx.asValue(Structured[Json]).serde

  test("serializer should serialize Json to UTF-8 bytes") {
    val json = parse("""{"name":"test","value":123}""").toOption.get

    val bytes = serde.serializer.serialize("topic", json)

    new String(bytes, "UTF-8") shouldBe """{"name":"test","value":123}"""
  }

  test("serializer should return null for null input") {
    serde.serializer.serialize("topic", null) shouldBe null
  }

  test("deserializer should deserialize valid JSON bytes") {
    val jsonString = """{"name":"test","value":123}"""
    val bytes = jsonString.getBytes("UTF-8")

    val result = serde.deserializer.deserialize("topic", bytes)

    result shouldBe parse(jsonString).toOption.get
  }

  test("deserializer should return null for null input") {
    serde.deserializer.deserialize("topic", null) shouldBe null
  }

  test("deserializer should throw SerializationException for invalid JSON") {
    val invalidJson = """{"name":"test","value":}"""
    val bytes = invalidJson.getBytes("UTF-8")

    val ex = intercept[SerializationException] {
      serde.deserializer.deserialize("topic", bytes)
    }

    ex.getMessage should include("Invalid:")
    ex.getCause should not be null
  }

  test("serializer should correctly serialize Json.Null") {
    val bytes = serde.serializer.serialize("topic", Json.Null)

    new String(bytes, "UTF-8") shouldBe "null"
  }

  test("deserializer should correctly deserialize JSON null") {
    val bytes = "null".getBytes("UTF-8")

    val result = serde.deserializer.deserialize("topic", bytes)

    result shouldBe Json.Null
  }

}
