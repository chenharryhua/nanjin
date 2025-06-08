package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.{immigrate, AvroCodecOf, KJson}
import io.circe.Json
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite

class NullTests extends AnyFunSuite {

  test("decode null should return null") {
    assert(intCodec.deserialize(null) === 0)
    assert(longCodec.deserialize(null) === 0)
    assert(doubleCodec.deserialize(null) === 0)
    assert(floatCodec.deserialize(null) === 0)
    assert(strCodec.deserialize(null) === null)
    assert(byteArrayCodec.deserialize(null) === null)
    assert(PrimitiveTypeCombined.primitiviesCodec.deserialize(null) === null)
    assert(PrimitiveTypeCombined.jsonPrimCodec.deserialize(null) === null)
  }

  test("tryDecode null should return failure") {
    assert(intCodec.tryDeserialize(null).isFailure)
    assert(longCodec.tryDeserialize(null).isFailure)
    assert(doubleCodec.tryDeserialize(null).isFailure)
    assert(floatCodec.tryDeserialize(null).isFailure)
    assert(strCodec.tryDeserialize(null).isFailure)
    assert(byteArrayCodec.tryDeserialize(null).isFailure)
    assert(PrimitiveTypeCombined.primitiviesCodec.tryDeserialize(null).isFailure)
    assert(PrimitiveTypeCombined.jsonPrimCodec.tryDeserialize(null).isFailure)
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
    val js = AvroCodecOf[KJson[Json]]
    assert(js.serializer.serialize("", null) == null)
    assert(js.serializer.serialize("", KJson(null)) == null)
    assert(js.deserializer.deserialize("", null) == null)
    assert(js.avroCodec.encode(null) == null)
    assert(js.avroCodec.encode(KJson(null)) == null)
    assert(js.avroCodec.decode(null) == null)
  }
}
