package mtest

import com.github.chenharryhua.nanjin.codec.CodecException.InvalidObjectException
import com.github.chenharryhua.nanjin.codec.SerdeOf
import org.scalatest.funsuite.AnyFunSuite

final case class Foo(a: String, b: Int)
final case class Bar(a: Int, b: String)

class KafkaAvroTest extends AnyFunSuite {
  val fooCodec = SerdeOf[Foo].asValue(sr).codec("avro.test")
  val barCode  = SerdeOf[Bar].asKey(sr).codec("avro.test")
  test("should throw InvalidObjectException when codec does not match") {
    assertThrows[InvalidObjectException](barCode.decode(fooCodec.encode(Foo("a", 0))))
  }
}
