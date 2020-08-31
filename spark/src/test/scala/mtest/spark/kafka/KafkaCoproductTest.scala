package mtest.spark.kafka

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.kafka._
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import shapeless._

object KafkaCoproductData {
  final case class Child1(a: Int, b: String)
  final case class Child2(a: Int, b: String)

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

  test("sparKafka not work with case object -- task serializable issue(avro4s) - happy failure") {
    val data = List(topicCO.fs2PR(0, co1), topicCO.fs2PR(1, co2))
    val path = "./data/test/spark/kafka/coproduct/caseobject.avro"
    val sk   = topicCO.sparKafka
    import sk.optionalKVCodec

    val run = topicCO.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicCO.schemaRegister >>
      topicCO.send(data) >>
      sk.fromKafka.flatMap(_.save.single.avro(path).run(blocker)) >>
      IO(loaders.raw.avro(path).collect().toSet)
    intercept[Exception](run.unsafeRunSync().flatMap(_.value) == Set(co1, co2))
  }

  test("sparKafka should be sent to kafka and save to single avro") {
    val data = List(topicEnum.fs2PR(0, en1), topicEnum.fs2PR(1, en2))
    val path = "./data/test/spark/kafka/coproduct/scalaenum.avro"
    val sk   = topicEnum.sparKafka
    import sk.optionalKVCodec

    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicEnum.schemaRegister >>
      topicEnum.send(data) >>
      sk.fromKafka.flatMap(_.save.single.avro(path).run(blocker)) >>
      IO(loaders.raw.avro(path).take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }

  test("sparKafka should be sent to kafka and save to multi avro") {
    val data = List(topicEnum.fs2PR(0, en1), topicEnum.fs2PR(1, en2))
    val path = "./data/test/spark/kafka/coproduct/multi-scalaenum.avro"
    val sk   = topicEnum.sparKafka
    import sk.optionalKVCodec

    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicEnum.schemaRegister >>
      topicEnum.send(data) >>
      sk.fromKafka.flatMap(_.save.multi.raw.avro(path).run(blocker)) >>
      IO(loaders.raw.avro(path).take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }

}
