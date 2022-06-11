package mtest.msg.codec

import org.scalatest.funsuite.AnyFunSuite

class NullTests extends AnyFunSuite {

  test("decode null should return null") {
    assert(intCodec.decode(null) === 0)
    assert(longCodec.decode(null) === 0)
    assert(doubleCodec.decode(null) === 0)
    assert(floatCodec.decode(null) === 0)
    assert(strCodec.decode(null) === null)
    assert(byteArrayCodec.decode(null) === null)
    assert(PrimitiveTypeCombined.primitiviesCodec.decode(null) === null)
    assert(PrimitiveTypeCombined.jsonPrimCodec.decode(null) === null)
  }

  test("tryDecode null should return failure") {
    assert(intCodec.tryDecode(null).isFailure)
    assert(longCodec.tryDecode(null).isFailure)
    assert(doubleCodec.tryDecode(null).isFailure)
    assert(floatCodec.tryDecode(null).isFailure)
    assert(strCodec.tryDecode(null).isFailure)
    assert(byteArrayCodec.tryDecode(null).isFailure)
    assert(PrimitiveTypeCombined.primitiviesCodec.tryDecode(null).isFailure)
    assert(PrimitiveTypeCombined.jsonPrimCodec.tryDecode(null).isFailure)
  }

  test("encode null should return null") {
    // assert(intCodec.encode(null) === null)
    // assert(longCodec.encode(null) === null)
    // assert(doubleCodec.encode(null) === null)
    // assert(floatCodec.encode(null) === null)
    assert(strCodec.encode(null) === null)
    assert(byteArrayCodec.encode(null) === null)
    assert(PrimitiveTypeCombined.primitiviesCodec.encode(null) === null)
    assert(PrimitiveTypeCombined.jsonPrimCodec.encode(null) === null)
  }
}
