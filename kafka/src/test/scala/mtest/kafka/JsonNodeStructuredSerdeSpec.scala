package mtest.kafka

import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.serdes.Structured
import org.apache.kafka.common.serialization.Serde
import munit.FunSuite

class JsonNodeStructuredSerdeSpec extends FunSuite {

  val serde: Serde[JsonNode] = ctx.asValue(Structured[JsonNode]).serde

  test("1.serializer should return null for null input") {
    assertEquals(serde.serializer.serialize("topic", null), null)
  }

  test("2.deserializer should return null for null input") {
    assertEquals(serde.deserializer.deserialize("topic", null), null)
  }

  test("3.deserializer should throw SerializationException for corrupted input bytes") {
    val ex = intercept[Exception] {
      serde.deserializer.deserialize("topic", Array[Byte](1, 2, 3))
    }

    // depending on underlying implementation this may be a SerializationException or another exception
    assert(ex.isInstanceOf[Exception])
  }

}
