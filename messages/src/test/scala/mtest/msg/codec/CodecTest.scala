package mtest.msg.codec

import com.github.chenharryhua.nanjin.messages.kafka.codec.{KafkaSerde, SerdeOf}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Success

object CodecTestData {
  final case class Foo(a: String, b: Int)
  final case class Bar(a: Int, b: String)
  val fooCodec: KafkaSerde[Foo] = SerdeOf[Foo].asValue(sr).topic("avro.test")
  val barCodec: KafkaSerde[Bar] = SerdeOf[Bar].asKey(sr).topic("avro.test")
}

class CodecTest extends AnyFunSuite {
  import CodecTestData._

  test("tryDecode should fail if codec not match") {
    assert(barCodec.tryDeserialize(fooCodec.serialize(Foo("a", 0))).isFailure)
  }

  test("tryDecode should be identical") {
    val foo = Foo("a", 0)
    assert(fooCodec.tryDeserialize(fooCodec.serialize(foo)) === Success(foo))
  }

  test("should throw InvalidObjectException when codec does not match") {
    assertThrows[Exception](barCodec.deserialize(fooCodec.serialize(Foo("a", 0))))
  }
}
