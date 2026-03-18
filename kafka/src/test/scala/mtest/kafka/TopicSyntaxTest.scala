package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.TopicName
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.serdes.Primitive

class TopicSyntaxTest extends AnyFunSuite {
  test("topic name") {
    val tn: TopicName = "abc.unsafe"
    val tn2 = TopicName("abc.checked")
    println((tn, tn2))
  }

  test("consume") {
    val k = ctx.asKey(Primitive[Integer]).deserializer[IO].attempt
    val v = ctx.asValue(Primitive[Integer]).deserializer[IO].option

    val res = ctx.consume("topic", k, v)
    println(res)
  }
  test("producer") {
    val k = ctx.asKey(Primitive[Integer]).serializer[IO]
    val v = ctx.asValue(Primitive[Integer]).serializer[IO].option

    val res = ctx.produce("topic", k, v)
    println(res)
  }
}
