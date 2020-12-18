package example

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaTopic

final case class Foo(a: Int, b: String)
final case class Bar(c: Int, d: Long)
final case class FooBar(e: Int, f: String)

object topics {
  val fooTopic: KafkaTopic[IO, Int, Foo]       = ctx.topic[Int, Foo]("example.foo")
  val barTopic: KafkaTopic[IO, Int, Bar]       = ctx.topic[Int, Bar]("example.bar")
  val foobarTopic: KafkaTopic[IO, Int, FooBar] = ctx.topic[Int, FooBar]("example.foobar")
}
