package mtest.msg.codec

import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite

class NullTests extends AnyFunSuite {

  test("decode null should return null") {
    assert(intCodec.decode(null) === 0)
    assert(longCodec.decode(null) === 0)
    assert(doubleCodec.decode(null) === 0)
    assert(floatCodec.decode(null) === 0)
    assert(strCodec.decode(null) === null)
    assert(byteArrayCodec.decode(null) === null)
    assert(primitiviesCodec.decode(null) === null)
    assert(jsonPrimCodec.decode(null) === null)
  }

  test("tryDecode null should return failure") {
    assert(intCodec.tryDecode(null).isFailure)
    assert(longCodec.tryDecode(null).isFailure)
    assert(doubleCodec.tryDecode(null).isFailure)
    assert(floatCodec.tryDecode(null).isFailure)
    assert(strCodec.tryDecode(null).isFailure)
    assert(byteArrayCodec.tryDecode(null).isFailure)
    assert(primitiviesCodec.tryDecode(null).isFailure)
    assert(jsonPrimCodec.tryDecode(null).isFailure)
  }

  test("prism getOption of null should return Some(null)") {
    assert(intCodec.prism.getOption(null) === Some(0))
    assert(longCodec.prism.getOption(null) === Some(0))
    assert(doubleCodec.prism.getOption(null) === Some(0))
    assert(floatCodec.prism.getOption(null) === Some(0))
    assert(strCodec.prism.getOption(null) === Some(null))
    assert(byteArrayCodec.prism.getOption(null) === Some(null))
    assert(primitiviesCodec.prism.getOption(null) === Some(null))
    assert(jsonPrimCodec.prism.getOption(null) === Some(null))
  }

  test("encode null should return null") {
    // assert(intCodec.encode(null) === null)
    // assert(longCodec.encode(null) === null)
    // assert(doubleCodec.encode(null) === null)
    // assert(floatCodec.encode(null) === null)
    assert(strCodec.encode(null) === null)
    assert(byteArrayCodec.encode(null) === null)
    assert(primitiviesCodec.encode(null) === null)
    assert(jsonPrimCodec.encode(null) === null)
  }
}
