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
  val co1 = Person("zz", Addr1)
  val co2 = Person("ff", Addr2)

  final case class Person2(name: String, phoneType: PhoneType.Value)
  val en1 = Person2("cc", PhoneType.F)
  val en2 = Person2("ww", PhoneType.Z)

  final case class Person3(name: String, co: CoParent)
  val cp1 = Person3("aa", Coproduct[CoParent](Child1(1, "a")))
  val cp2 = Person3("bb", Coproduct[CoParent](Child2(2, "b")))

  val topic  = ctx.topic[Int, Person](TopicName("coproduct.person"))
  val topic2 = ctx.topic[Int, Person2](TopicName("coproduct.person2"))
  val topic3 = ctx.topic[Int, Person3](TopicName("coproduct.person3"))

}

class SparkCoproductTest extends AnyFunSuite {
  import sparkCoproduct._

  test("happy to see one day it fails") {
    illTyped(""" implicitly[TypedEncoder[Parent]] """)
    illTyped(""" implicitly[TypedEncoder[CoParent]] """)
    illTyped(""" implicitly[TypedEncoder[Address]] """)
    illTyped(""" implicitly[TypedEncoder[PhoneType.Value]] """)
  }

  ignore("work well with case object -- task serializable issue") {
    val run = topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topic.schemaRegistry.register >>
      topic.send(1, co1) >> topic.send(2, co2) >> topic.sparKafka.fromKafka.flatMap(_.save)
    run.unsafeRunSync()
  }

  test("do not work with scala Enumeration") {
    val run = topic2.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topic2.schemaRegistry.register >>
      topic2.send(1, en1) >> topic2.send(2, en2) >> topic2.sparKafka.fromKafka.flatMap(_.save)
    run.unsafeRunSync()
  }

  test("do not work with coproduct") {
    val run = topic3.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topic3.schemaRegistry.register >>
      topic3.send(1, cp1) >> topic3.send(2, cp2) >> topic3.sparKafka.fromKafka.flatMap(_.save)
    run.unsafeRunSync()
  }
}
