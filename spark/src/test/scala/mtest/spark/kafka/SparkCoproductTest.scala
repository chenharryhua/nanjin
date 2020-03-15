package mtest.spark.kafka

import com.github.chenharryhua.nanjin.kafka.TopicName
import org.scalatest.funsuite.AnyFunSuite
import shapeless._
import shapeless.test.illTyped
import mtest.spark.kafka.sparkCoproduct._
import frameless.TypedEncoder
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.kafka._

object sparkCoproduct {
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

  final case class Person(name: String, addr: Address)
  val p1 = Person("zz", Addr1)
  val p2 = Person("ff", Addr2)

  final case class Person2(name: String, phoneType: PhoneType.Value)
  val f1 = Person2("cc", PhoneType.F)
  val f2 = Person2("ww", PhoneType.Z)

  val topic  = ctx.topic[Int, Person](TopicName("coproduct.person"))
  val topic2 = ctx.topic[Int, Person2](TopicName("coproduct.person2"))

}

class SparkCoproductTest extends AnyFunSuite {
  import sparkCoproduct._

  test("happy to see one day it fails") {
    illTyped(""" implicitly[TypedEncoder[Parent]] """)
    illTyped(""" implicitly[TypedEncoder[CoParent]] """)
    illTyped(""" implicitly[TypedEncoder[Address]] """)
    illTyped(""" implicitly[TypedEncoder[PhoneType.Value]] """)
  }

  test("work well with case object") {
    val run = topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topic.schemaRegistry.register >>
      topic.send(1, p1) >> topic.send(2, p2) >> topic.sparKafka.save
    run.unsafeRunSync()
  }

  ignore("do not work with scala Enumeration") {
    val run = topic2.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topic2.schemaRegistry.register >>
      topic2.send(1, f1) >> topic2.send(2, f2) >> topic2.sparKafka.save
    run.unsafeRunSync()
  }
}
