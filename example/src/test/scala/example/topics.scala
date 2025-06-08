package example

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import eu.timepit.refined.auto.*
import frameless.TypedEncoder
import io.circe.Codec
final case class Foo(a: Int, b: String)

object Foo {
  implicit val codec: Codec[Foo] = io.circe.generic.semiauto.deriveCodec[Foo]
  implicit val foo: TypedEncoder[Foo] = shapeless.cachedImplicit
}
final case class Bar(c: Int, d: Long)
final case class FooBar(e: Int, f: String)

object topics {
  val fooTopic: TopicDef[Int, Foo] = TopicDef[Int, Foo](TopicName("example.foo"))
  val barTopic: TopicDef[Int, Bar] = TopicDef[Int, Bar](TopicName("example.bar"))
  val foobarTopic: TopicDef[Int, FooBar] = TopicDef[Int, FooBar](TopicName("example.foobar"))
}
