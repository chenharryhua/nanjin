package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.shapes._
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite
import shapeless._

object CoproductJsons {
  final case class Foo(a: Int, b: String)
  final case class Bar(a: Int, b: String)

  type FooBar = Foo :+: Bar :+: CNil
  final case class FB(fb: FooBar, c: Int)
}

class CoproductJsonTest extends AnyFunSuite {
  import CoproductJsons._
  val foo   = Foo(1, "foo-1")
  val bar   = Bar(2, "bar-2")
  val fb1   = FB(Coproduct[FooBar](foo), 0)
  val fb2   = FB(Coproduct[FooBar](bar), 1)
  val topic = TopicDef[Int, FB]("coproduct.test").in(ctx)

  test("circe json coproduct is not invertable") {
    assert(decode[FB](fb1.asJson.noSpaces).toOption.get === fb1)
    assert(decode[FB](fb2.asJson.noSpaces).toOption.get !== fb2)
  }

  test("jackson json coproduct is invertable. witness by toJackson/fromJackson") {
    val msg1: NJConsumerRecord[Int, FB] =
      NJConsumerRecord(0, 0, 0, Some(0), Some(fb1), "coproduct.test", 0)
    val msg2: NJConsumerRecord[Int, FB] =
      NJConsumerRecord(0, 0, 0, Some(0), Some(fb2), "coproduct.test", 0)

    val mfb1 = topic.description
      .fromJackson(topic.description.topicDef.toJackson(msg1).noSpaces)
      .toOption
      .get
      .value
      .get
    val mfb2 = topic.description
      .fromJackson(topic.description.topicDef.toJackson(msg2).noSpaces)
      .toOption
      .get
      .value
      .get

    assert(mfb1 === fb1)
    assert(mfb2 === fb2)
  }
}
