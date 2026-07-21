package mtest.kafka

import com.google.protobuf.DynamicMessage
import com.github.chenharryhua.nanjin.kafka.serdes.Structured
import org.apache.kafka.common.serialization.Serde
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DynamicMessageStructuredSpec extends AnyFunSuite with Matchers {

  val serde: Serde[DynamicMessage] = ctx.asValue(Structured[DynamicMessage]).serde

  test("serializer should return null for null input") {
    serde.serializer.serialize("topic", null) shouldBe null
  }

  test("deserializer should return null for null input") {
    serde.deserializer.deserialize("topic", null) shouldBe null
  }

  // Full round-trip tests for DynamicMessage are covered in integration-style tests (UpAndDownProtoTest).
}
