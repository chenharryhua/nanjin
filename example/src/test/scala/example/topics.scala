package example

import com.github.chenharryhua.nanjin.kafka.serdes.{Primitive, Structured}
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import io.circe.Codec
import io.github.iltotore.iron.*
import org.apache.avro.generic.GenericRecord

final case class Foo(a: Int, b: String)

object Foo {
  implicit val codec: Codec[Foo] = io.circe.generic.semiauto.deriveCodec[Foo]
}
final case class Bar(c: Int, d: Long)
final case class FooBar(e: Int, f: String)

object topics {
  val foo = Structured[GenericRecord].become[Foo]
  val bar = Structured[GenericRecord].become[Bar]

  val fooTopic: TopicDef[Integer, Foo] =
    TopicDef[Integer, Foo](TopicName("example.foo"), Primitive[Integer], foo)
  val barTopic: TopicDef[Integer, Bar] =
    TopicDef[Integer, Bar](TopicName("example.bar"), Primitive[Integer], bar) // compile time check
  // val foobarTopic: TopicDef[Integer, FooBar] = TopicDef[Integer, FooBar]("example.foobar",Primitive[Integer]) // runtime check
}
