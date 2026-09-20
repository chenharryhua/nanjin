package mtest.kafka

import com.google.protobuf.DynamicMessage
import com.github.chenharryhua.nanjin.kafka.serdes.Structured
import org.apache.kafka.common.serialization.Serde
import munit.FunSuite

class DynamicMessageStructuredSpec extends FunSuite {

  val serde: Serde[DynamicMessage] = ctx.asValue(Structured[DynamicMessage]).serde

  test("1.serializer should return null for null input") {
    assertEquals(serde.serializer.serialize("topic", null), null)
  }

  test("2.deserializer should return null for null input") {
    assertEquals(serde.deserializer.deserialize("topic", null), null)
  }

  // Full round-trip tests for DynamicMessage are covered in integration-style tests (UpAndDownProtoTest).
}
