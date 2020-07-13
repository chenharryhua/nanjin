package mtest.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJCodec, SerdeOf}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Success

object CodecTestData {
  final case class Foo(a: String, b: Int)
  final case class Bar(a: Int, b: String)
  val fooCodec: NJCodec[Foo] = SerdeOf[Foo].asValue(sr).codec("avro.test")
  val barCodec: NJCodec[Bar] = SerdeOf[Bar].asKey(sr).codec("avro.test")
}

class CodecTest extends AnyFunSuite {
  import CodecTestData._

  test("tryDecode should fail if codec not match") {
    assert(barCodec.tryDecode(fooCodec.encode(Foo("a", 0))).isFailure)
  }

  test("tryDecode should be identical") {
    val foo = Foo("a", 0)
    assert(fooCodec.tryDecode(fooCodec.encode(foo)) === Success(foo))
  }

  test("should throw InvalidObjectException when codec does not match") {
    assertThrows[Exception](barCodec.decode(fooCodec.encode(Foo("a", 0))))
  }
}
