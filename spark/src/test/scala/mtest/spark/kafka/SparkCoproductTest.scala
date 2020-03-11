package mtest.spark.kafka

import org.scalatest.funsuite.AnyFunSuite
import shapeless._
import shapeless.test.illTyped
import mtest.spark.kafka.sparkCoproduct._
import frameless.TypedEncoder

object sparkCoproduct {
  sealed trait Parent
  final case class Child1(a: Int, b: String) extends Parent
  final case class Child2(a: Int, b: String) extends Parent
  final case class GrandChild(a: Child1, b: Child2) extends Parent

  type CoParent = Child1 :+: Child2 :+: CNil

  sealed trait EnumLike
  object Enum1 extends EnumLike
  object Enum2 extends EnumLike

  object PhoneType extends Enumeration {
    type PhoneType = Value
    val F, H, M, W = Value
  }
}

class SparkCoproductTest extends AnyFunSuite {

  test("happy to see one day it fails") {
    illTyped("""implicitly[TypedEncoder[Parent]]""")
    illTyped("""implicitly[TypedEncoder[CoParent]]""")
    illTyped(""" implicitly[TypedEncoder[EnumLike]] """)
    illTyped(""" implicitly[TypedEncoder[PhoneType.PhoneType]] """)
  }
}
