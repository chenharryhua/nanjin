package example

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import eu.timepit.refined.auto.*
import io.circe.Codec
final case class Foo(a: Int, b: String)

object Foo {
  implicit val codec: Codec[Foo] = io.circe.generic.semiauto.deriveCodec[Foo]
}
final case class Bar(c: Int, d: Long)
final case class FooBar(e: Int, f: String)

object topics {
  val fooTopic: AvroTopic[Int, Foo] = AvroTopic[Int, Foo](TopicName("example.foo"))
  val barTopic: AvroTopic[Int, Bar] = AvroTopic[Int, Bar](TopicName("example.bar"))
  val foobarTopic: AvroTopic[Int, FooBar] = AvroTopic[Int, FooBar](TopicName("example.foobar"))
}
