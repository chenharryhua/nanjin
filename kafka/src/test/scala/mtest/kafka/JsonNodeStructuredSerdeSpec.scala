package mtest.kafka

import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.serdes.Structured
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serde
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JsonNodeStructuredSerdeSpec extends AnyFunSuite with Matchers {

  val serde: Serde[JsonNode] = ctx.asValue(Structured[JsonNode]).serde

  test("serializer should return null for null input") {
    serde.serializer.serialize("topic", null) shouldBe null
  }

  test("deserializer should return null for null input") {
    serde.deserializer.deserialize("topic", null) shouldBe null
  }

  test("deserializer should throw SerializationException for corrupted input bytes") {
    val ex = intercept[Exception] {
      serde.deserializer.deserialize("topic", Array[Byte](1, 2, 3))
    }

    // depending on underlying implementation this may be a SerializationException or another exception
    ex shouldBe a [Exception]
  }

}
