package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.{immigrate, AvroFor, KJson}
import io.circe.Json
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite

class NullTests extends AnyFunSuite {

  test("decode null should return null") {
    assert(intCodec.deserialize(null) === null)
    assert(longCodec.deserialize(null) === null)
    assert(doubleCodec.deserialize(null) === null)
    assert(floatCodec.deserialize(null) === null)
    assert(strCodec.deserialize(null) === null)
    assert(byteArrayCodec.deserialize(null) === null)
    assert(PrimitiveTypeCombined.primitiviesCodec.deserialize(null) === null)
    assert(PrimitiveTypeCombined.jsonPrimCodec.deserialize(null) === null)
  }

  test("tryDecode null should return success") {
    assert(intCodec.tryDeserialize(null).isSuccess)
    assert(longCodec.tryDeserialize(null).isSuccess)
    assert(doubleCodec.tryDeserialize(null).isSuccess)
    assert(floatCodec.tryDeserialize(null).isSuccess)
    assert(strCodec.tryDeserialize(null).isSuccess)
    assert(byteArrayCodec.tryDeserialize(null).isSuccess)
    assert(PrimitiveTypeCombined.primitiviesCodec.tryDeserialize(null).isSuccess)
    assert(PrimitiveTypeCombined.jsonPrimCodec.tryDeserialize(null).isSuccess)
  }

  test("encode null should return null") {
    // assert(intCodec.encode(null) === null)
    // assert(longCodec.encode(null) === null)
    // assert(doubleCodec.encode(null) === null)
    // assert(floatCodec.encode(null) === null)
    assert(strCodec.serialize(null) === null)
    assert(byteArrayCodec.serialize(null) === null)
    assert(PrimitiveTypeCombined.primitiviesCodec.serialize(null) === null)
    assert(PrimitiveTypeCombined.jsonPrimCodec.serialize(null) === null)
  }

  test("immigrate null") {
    assert(immigrate(Schema.create(Schema.Type.INT), null).get == null)
  }

  test("kjson codec null") {
    val js = AvroFor[KJson[Json]]
    assert(js.asKey(Map.empty).serde.serializer.serialize("", null) == null)
    assert(js.asKey(Map.empty).serde.serializer.serialize("", KJson(null)) == null)
    assert(js.asKey(Map.empty).serde.deserializer.deserialize("", null) == null)
  }
}
