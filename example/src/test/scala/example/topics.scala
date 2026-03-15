package example

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import io.github.iltotore.iron.*
import io.circe.Codec
final case class Foo(a: Int, b: String)

object Foo {
  implicit val codec: Codec[Foo] = io.circe.generic.semiauto.deriveCodec[Foo]
}
final case class Bar(c: Int, d: Long)
final case class FooBar(e: Int, f: String)

object topics {
  val fooTopic: AvroTopic[Integer, Foo] = AvroTopic[Integer, Foo](TopicName("example.foo"))
  val barTopic: AvroTopic[Integer, Bar] = AvroTopic[Integer, Bar](TopicName("example.bar"))
  val foobarTopic: AvroTopic[Integer, FooBar] = AvroTopic[Integer, FooBar](TopicName("example.foobar"))
}
