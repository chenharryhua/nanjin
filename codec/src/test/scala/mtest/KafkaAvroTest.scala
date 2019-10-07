package mtest

import com.github.chenharryhua.nanjin.codec.CodecException.InvalidObjectException
import com.github.chenharryhua.nanjin.codec.{KafkaSerdeAvro, SerdeOf}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._
import scala.util.Success
final case class Foo(a: String, b: Int)
final case class Bar(a: Int, b: String)

class KafkaAvroTest extends AnyFunSuite {
  val fooCodec = SerdeOf[Foo].asValue(sr).codec("avro.test")
  val barCode  = SerdeOf[Bar].asKey(sr).codec("avro.test")

  test("tryDecode should fail if codec not match") {
    assert(barCode.tryDecode(fooCodec.encode(Foo("a", 0))).isFailure)
  }

  test("tryDecode should be identical") {
    val foo = Foo("a", 0)
    assert(fooCodec.tryDecode(fooCodec.encode(foo)) === Success(foo))
  }

  test("should throw InvalidObjectException when codec does not match") {
    assertThrows[InvalidObjectException](barCode.decode(fooCodec.encode(Foo("a", 0))))
  }
}
