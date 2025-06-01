package example

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef}
import frameless.TypedEncoder
import io.circe.Codec
import eu.timepit.refined.auto.*
final case class Foo(a: Int, b: String)

object Foo {
  implicit val codec: Codec[Foo]      = io.circe.generic.semiauto.deriveCodec[Foo]
  implicit val foo: TypedEncoder[Foo] = shapeless.cachedImplicit
}
final case class Bar(c: Int, d: Long)
final case class FooBar(e: Int, f: String)

object topics {
  val fooTopic: KafkaTopic[IO, Int, Foo]       = ctx.topic(TopicDef[Int, Foo](TopicName("example.foo")))
  val barTopic: KafkaTopic[IO, Int, Bar]       = ctx.topic(TopicDef[Int, Bar](TopicName("example.bar")))
  val foobarTopic: KafkaTopic[IO, Int, FooBar] = ctx.topic(TopicDef[Int, FooBar](TopicName("example.foobar")))
}
