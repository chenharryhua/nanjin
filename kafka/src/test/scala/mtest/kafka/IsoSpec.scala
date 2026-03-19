package mtest.kafka

import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.serdes.*
import io.circe.Json
import io.scalaland.chimney.Iso
import org.apache.avro.generic.GenericRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer

// Dummy case classes for testing
case class TestAvro(id: Int, name: String)
case class TestJson(field1: String, field2: Int)

class IsoSpec extends AnyFlatSpec with Matchers {

  "avro4s Iso" should "round-trip a case class through GenericRecord" in {

    val iso: Iso[GenericRecord, TestAvro] = isoGenericRecord[TestAvro]
    val orig = TestAvro(42, "hello")

    val record: GenericRecord = iso.second.transform(orig)
    val back: TestAvro = iso.first.transform(record)

    back shouldEqual orig
  }

  "jackson Iso" should "round-trip a case class through JsonNode" in {
    val orig = TestJson("foo", 123)
    val iso: Iso[JsonNode, TestJson] = isoJsonNode[TestJson]

    val node: JsonNode = iso.second.transform(orig)
    val back: TestJson = iso.first.transform(node)

    back shouldEqual orig
  }

  "jsonByteBuffer Iso" should "round-trip Json through ByteBuffer" in {
    val json: Json = Json.obj("foo" -> Json.fromString("bar"), "baz" -> Json.fromInt(42))
    val iso = isoByteBufferJson

    val buf: ByteBuffer = iso.second.transform(json)
    val back: Json = iso.first.transform(buf)

    back shouldEqual json
  }

  it should "throw on invalid ByteBuffer" in {
    val iso = isoByteBufferJson
    val invalid = ByteBuffer.wrap("not json".getBytes())

    an[Exception] should be thrownBy iso.first.transform(invalid)
  }
}
