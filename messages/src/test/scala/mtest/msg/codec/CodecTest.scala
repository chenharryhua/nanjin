package mtest.msg.codec

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, AvroFor, KafkaSerde}
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

object CodecTestData {
  final case class Foo(a: String, b: Int)
  final case class Bar(a: Int, b: String)
  val fooCodec: KafkaSerde[Foo] = AvroFor[Foo].asValue(sr).withTopic(TopicName("avro.test"))
  val barCodec: KafkaSerde[Bar] = AvroFor[Bar].asKey(sr).withTopic(TopicName("avro.test"))
}

class CodecTest extends AnyFunSuite {
  import CodecTestData.*

  test("should throw InvalidObjectException when codec does not match") {
    assertThrows[Exception](barCodec.deserialize(fooCodec.serialize(Foo("a", 0))))
  }

  test("primitive types are illegal") {
    shapeless.test.illTyped(""" AvroCodec[Int] """)
    shapeless.test.illTyped(""" AvroCodec[String] """)
    shapeless.test.illTyped(""" AvroCodec[Long] """)
    shapeless.test.illTyped(""" AvroCodec[Short] """)
    AvroCodec[Foo]
    AvroCodec[Bar]
  }
}
