package mtest.spark

import cats.effect.IO
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil}
import shapeless.test.illTyped

object SparkCoproductTestData {
  sealed trait Parent
  final case class Child1(a: Int, b: String) extends Parent
  final case class Child2(a: Int, b: String) extends Parent
  final case class GrandChild(a: Child1, b: Child2) extends Parent

  type CoParent = Child1 :+: Child2 :+: CNil

  sealed trait Address
  case object Addr1 extends Address
  case object Addr2 extends Address

  object PhoneType extends Enumeration {
    val F, Z = Value
  }
}

class SparkCoproductTest extends AnyFunSuite {
  test("spark frameless does not directly support coproduct yet - wonderful if fail") {
    illTyped(""" implicitly[TypedEncoder[Parent]] """)
    illTyped(""" implicitly[TypedEncoder[CoParent]] """)
    illTyped(""" implicitly[TypedEncoder[Address]] """)
  }
}

final class SerializableIoRdd(ioRdd: IO[RDD[Int]]) extends Serializable {
  def count: IO[Long] = ioRdd.map(_.count())
}
final class SerializableIoDS(ioRdd: IO[Dataset[Int]]) extends Serializable {
  def count: IO[Long] = ioRdd.map(_.count())
}
