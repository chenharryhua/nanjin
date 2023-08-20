package mtest.msg.codec

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
}
