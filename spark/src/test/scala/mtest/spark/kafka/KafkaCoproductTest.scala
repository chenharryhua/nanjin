package mtest.spark.kafka

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import shapeless._

object KafkaCoproductData {
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

  final case class PersonCaseObject(name: String, addr: Address)
  val co1: PersonCaseObject = PersonCaseObject("zz", Addr1)
  val co2: PersonCaseObject = PersonCaseObject("ff", Addr2)

  final case class PersonEnum(name: String, phoneType: PhoneType.Value)
  val en1: PersonEnum = PersonEnum("cc", PhoneType.F)
  val en2: PersonEnum = PersonEnum("ww", PhoneType.Z)

  final case class PersonCoproduct(name: String, co: CoParent)
  val cp1: PersonCoproduct = PersonCoproduct("aa", Coproduct[CoParent](Child1(1, "a")))
  val cp2: PersonCoproduct = PersonCoproduct("bb", Coproduct[CoParent](Child2(2, "b")))

  val topicCO: KafkaTopic[IO, Int, PersonCaseObject] =
    ctx.topic[Int, PersonCaseObject]("test.spark.kafka.coproduct.caseobject")

  val topicEnum: KafkaTopic[IO, Int, PersonEnum] =
    ctx.topic[Int, PersonEnum]("test.spark.kafka.coproduct.scalaenum")

  val topicCoProd: KafkaTopic[IO, Int, PersonCoproduct] =
    ctx.topic[Int, PersonCoproduct]("test.spark.kafka.coproduct.shapelesscoproduct")

}

class KafkaCoproductTest extends AnyFunSuite {
  import KafkaCoproductData._

  test("not work with case object -- task serializable issue(avro4s) - happy failure") {
    val run = topicCO.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicCO.schemaRegister >>
      topicCO.send(1, co1) >> topicCO
      .send(2, co2) >> topicCO.sparKafka(range).fromKafka.flatMap(_.saveAvro(blocker))
    intercept[Exception](run.unsafeRunSync())
  }

  test("should be sent to kafka and save to parquet") {
    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicEnum.schemaRegister >>
      topicEnum.send(1, en1) >> topicEnum
      .send(2, en2) >> topicEnum.sparKafka(range).fromKafka.flatMap(_.saveParquet(blocker))
    assert(run.unsafeRunSync() == 2)
  }

  test("should be sent to kafka and save to jackson") {
    val run = topicCoProd.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicCoProd.schemaRegister >>
      topicCoProd.send(1, cp1) >> topicCoProd
      .send(2, cp2) >> topicCoProd.sparKafka(range).fromKafka.flatMap(_.saveJackson(blocker))
    assert(run.unsafeRunSync() == 2)
  }
}
