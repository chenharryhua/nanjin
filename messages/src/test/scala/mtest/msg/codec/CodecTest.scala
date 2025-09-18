package mtest.msg.codec

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, AvroFor, KafkaSerde}
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Success

object CodecTestData {
  final case class Foo(a: String, b: Int)
  final case class Bar(a: Int, b: String)
  val fooCodec: KafkaSerde[Foo] = AvroFor[Foo].asValue(sr).withTopic(TopicName("avro.test"))
  val barCodec: KafkaSerde[Bar] = AvroFor[Bar].asKey(sr).withTopic(TopicName("avro.test"))
}

class CodecTest extends AnyFunSuite {
  import CodecTestData.*

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

  test("record of") {
    val codec = AvroCodec[Foo]
    assert(codec.decode(codec.recordOf(Foo("a", 0)).get).isInstanceOf[Foo])
  }
}
