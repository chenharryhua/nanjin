package mtest.msg.codec

import org.scalatest.funsuite.AnyFunSuite

object CoproductJsons {
  final case class Foo(a: Int, b: String)
  final case class Bar(a: Int, b: String)

  type FooBar = Foo | Bar
  final case class FB(fb: FooBar, c: Int)
}

class CoproductJsonTest extends AnyFunSuite {
  import CoproductJsons.*
  val foo: Foo = Foo(1, "foo-1")
  val bar: Bar = Bar(2, "bar-2")

  test("circe json coproduct is not invertible") {}
}
