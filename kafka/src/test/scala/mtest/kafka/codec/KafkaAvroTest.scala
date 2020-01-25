package mtest.kafka.codec

import com.github.chenharryhua.nanjin.kafka.common.TopicName
import com.github.chenharryhua.nanjin.kafka.codec.CodecException.InvalidObjectException
import com.github.chenharryhua.nanjin.kafka.codec.SerdeOf
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Success
final case class Foo(a: String, b: Int)
final case class Bar(a: Int, b: String)

class KafkaAvroTest extends AnyFunSuite {
  val fooCodec = SerdeOf[Foo].asValue(sr).codec(TopicName("avro.test"))
  val barCodec = SerdeOf[Bar].asKey(sr).codec(TopicName("avro.test"))

  test("tryDecode should fail if codec not match") {
    assert(barCodec.tryDecode(fooCodec.encode(Foo("a", 0))).isFailure)
  }

  test("tryDecode should be identical") {
    val foo = Foo("a", 0)
    assert(fooCodec.tryDecode(fooCodec.encode(foo)) === Success(foo))
  }

  test("should throw InvalidObjectException when codec does not match") {
    assertThrows[InvalidObjectException](barCodec.decode(fooCodec.encode(Foo("a", 0))))
  }
}
