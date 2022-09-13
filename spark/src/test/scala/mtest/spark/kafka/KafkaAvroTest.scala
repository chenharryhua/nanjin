package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.kafka.{ProducerRecord, ProducerRecords}
import io.circe.Codec
import io.circe.generic.auto.*
import org.scalatest.funsuite.AnyFunSuite
import shapeless.*

object KafkaAvroTestData {
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

  object PersonEnum {
    implicit val codec: Codec[PersonEnum] = io.circe.generic.semiauto.deriveCodec[PersonEnum]
  }
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

class KafkaAvroTest extends AnyFunSuite {
  import KafkaAvroTestData.*

  test("sparKafka not work with case object -- task serializable issue(avro4s) - happy failure") {
    val data = fs2
      .Stream(
        ProducerRecords(
          List(
            ProducerRecord(topicCO.topicName.value, 0, co1),
            ProducerRecord(topicCO.topicName.value, 1, co2))))
      .covary[IO]
      .through(topicCO.produce.updateConfig(_.withClientId("kafka.avro.test1")).pipe)
    val path = NJPath("./data/test/spark/kafka/coproduct/caseobject.avro")
    val sk   = sparKafka.topic(topicCO.topicDef)

    val run = topicCO.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt >>
      topicCO.schemaRegistry.register >>
      data.compile.drain >>
      sk.fromKafka.flatMap(_.output.avro(path).run) >>
      IO(sk.load.avro(path).rdd.collect().toSet)
    intercept[Exception](run.unsafeRunSync().flatMap(_.value) == Set(co1, co2))
  }

  test("sparKafka should be sent to kafka and save to single avro") {
    val data = fs2
      .Stream(
        ProducerRecords(
          List(
            ProducerRecord(topicEnum.topicName.value, 0, en1),
            ProducerRecord(topicEnum.topicName.value, 1, en2))))
      .covary[IO]
      .through(topicEnum.produce.updateConfig(_.withClientId("kafka.avro.test2")).pipe)
    val avroPath    = NJPath("./data/test/spark/kafka/coproduct/scalaenum.avro")
    val jacksonPath = NJPath("./data/test/spark/kafka/coproduct/scalaenum.jackson.json")
    val circePath   = NJPath("./data/test/spark/kafka/coproduct/scalaenum.circe.json")
    val sk          = sparKafka.topic(topicEnum.topicDef)

    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt >>
      topicEnum.schemaRegistry.register >>
      data.compile.drain >>
      sk.fromKafka.flatMap(_.output.avro(avroPath).run) >>
      sk.fromKafka.flatMap(_.output.jackson(jacksonPath).run) >>
      sk.fromKafka.flatMap(_.output.circe(circePath).run) >>
      IO(sk.load.avro(avroPath).rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))

  }

  test("sparKafka should be sent to kafka and save to multi avro") {
    val data = fs2
      .Stream(
        ProducerRecords(
          List(
            ProducerRecord(topicEnum.topicName.value, 0, en1),
            ProducerRecord(topicEnum.topicName.value, 1, en2))))
      .covary[IO]
      .through(topicEnum.produce.updateConfig(_.withClientId("kafka.avro.test3")).pipe)

    val path = NJPath("./data/test/spark/kafka/coproduct/multi-scalaenum.avro")
    val sk   = sparKafka.topic(topicEnum.topicDef)

    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt >>
      topicEnum.schemaRegistry.register >>
      data.compile.drain >>
      sk.fromKafka.flatMap(_.output.avro(path).run) >>
      IO(sk.load.avro(path).rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }

  test("should be sent to kafka and save to multi snappy avro") {
    val data = fs2
      .Stream(
        ProducerRecords(
          List(
            ProducerRecord(topicEnum.topicName.value, 0, en1),
            ProducerRecord(topicEnum.topicName.value, 1, en2))))
      .covary[IO]
      .through(topicEnum.produce.updateConfig(_.withClientId("kafka.avro.test4")).pipe)

    val path = NJPath("./data/test/spark/kafka/coproduct/multi-scalaenum.snappy.avro")
    val sk   = sparKafka.topic(topicEnum.topicDef)

    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt >>
      topicEnum.schemaRegistry.register >>
      data.compile.drain >>
      sk.fromKafka.flatMap(_.output.avro(path).snappy.run) >>
      IO(sk.load.avro(path).rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }
  test("should be sent to kafka and save to binary bzip2 avro") {
    val data = fs2
      .Stream(
        ProducerRecords(
          List(
            ProducerRecord(topicEnum.topicName.value, 0, en1),
            ProducerRecord(topicEnum.topicName.value, 1, en2))))
      .covary[IO]
      .through(topicEnum.produce.updateConfig(_.withClientId("kafka.avro.test5")).pipe)
    val path = NJPath("./data/test/spark/kafka/coproduct/scalaenum.avro.bzip2")
    val sk   = sparKafka.topic(topicEnum.topicDef)

    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt >>
      topicEnum.schemaRegistry.register >>
      data.compile.drain >>
      sk.fromKafka.flatMap(_.output.binAvro(path).bzip2.run) >>
      IO(sk.load.binAvro(path).rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }

}
