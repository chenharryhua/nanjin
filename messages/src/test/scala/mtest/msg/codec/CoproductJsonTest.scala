package mtest.msg.codec

import io.circe.generic.auto.*
import io.circe.jawn.decode
import io.circe.shapes.*
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite
import shapeless.*

object CoproductJsons {
  final case class Foo(a: Int, b: String)
  final case class Bar(a: Int, b: String)

  type FooBar = Foo :+: Bar :+: CNil
  final case class FB(fb: FooBar, c: Int)
}

class CoproductJsonTest extends AnyFunSuite {
  import CoproductJsons.*
  val foo: Foo = Foo(1, "foo-1")
  val bar: Bar = Bar(2, "bar-2")
  val fb1: FB = FB(Coproduct[FooBar](foo), 0)
  val fb2: FB = FB(Coproduct[FooBar](bar), 1)

  test("circe json coproduct is not invertible") {
    assert(decode[FB](fb1.asJson.noSpaces).toOption.get === fb1)
    assert(decode[FB](fb2.asJson.noSpaces).toOption.get !== fb2)
  }
}
