package mtest.spark.kafka

import org.scalatest.funsuite.AnyFunSuite
import shapeless._
import shapeless.test.illTyped

object sparkCoproduct {
  sealed trait Parent
  final case class Child1(a: Int, b: String) extends Parent
  final case class Child2(a: Int, b: String) extends Parent
  final case class GrandChild(a: Child1, b: Child2) extends Parent

  type CoParent = Child1 :+: Child2 :+: CNil
}

class SparkCoproductTest extends AnyFunSuite {
  test("happy to see one day it fails") {
    illTyped("""implicitly[TypedEncoder[Parent]]""")
    illTyped("""implicitly[TypedEncoder[CoParent]]""")
  }
}
