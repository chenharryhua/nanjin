package mtest.spark.kafka

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.injection._
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite
import shapeless._
import fs2.kafka.ProducerRecord
import fs2.kafka.ProducerRecords
import cats.effect.unsafe.implicits.global
import io.circe.Codec

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
  import KafkaAvroTestData._

  test("sparKafka not work with case object -- task serializable issue(avro4s) - happy failure") {
    val data = fs2
      .Stream(
        ProducerRecords(
          List(ProducerRecord(topicCO.topicName.value, 0, co1), ProducerRecord(topicCO.topicName.value, 1, co2))))
      .covary[IO]
      .through(topicCO.fs2Channel.updateProducer(_.withClientId("kafka.avro.test1")).producerPipe)
    val path = "./data/test/spark/kafka/coproduct/caseobject.avro"
    val sk   = sparKafka.topic(topicCO.topicDef)

    val run = topicCO.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicCO.schemaRegistry.register >>
      data.compile.drain >>
      sk.fromKafka.flatMap(_.save.avro(path).file.sink.compile.drain) >>
      sk.load.rdd.avro(path).map(_.rdd.collect().toSet)
    intercept[Exception](run.unsafeRunSync().flatMap(_.value) == Set(co1, co2))
  }

  test("sparKafka should be sent to kafka and save to single avro") {
    val data = fs2
      .Stream(
        ProducerRecords(
          List(ProducerRecord(topicEnum.topicName.value, 0, en1), ProducerRecord(topicEnum.topicName.value, 1, en2))))
      .covary[IO]
      .through(topicEnum.fs2Channel.updateProducer(_.withClientId("kafka.avro.test2")).producerPipe)
    val avroPath    = "./data/test/spark/kafka/coproduct/scalaenum.avro"
    val jacksonPath = "./data/test/spark/kafka/coproduct/scalaenum.jackson.json"
    val circePath   = "./data/test/spark/kafka/coproduct/scalaenum.circe.json"
    val sk          = sparKafka.topic(topicEnum.topicDef)

    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicEnum.schemaRegistry.register >>
      data.compile.drain >>
      sk.fromKafka.flatMap(_.save.avro(avroPath).file.sink.compile.drain) >>
      sk.fromKafka.flatMap(_.save.jackson(jacksonPath).file.sink.compile.drain) >>
      sk.fromKafka.flatMap(_.save.circe(circePath).file.sink.compile.drain) >>
      sk.load.rdd.avro(avroPath).map(_.rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))

    val avro =
      sk.load.stream.avro(avroPath, 100).mapFilter(_.value).compile.toList.unsafeRunSync().toSet
    assert(avro == Set(en1, en2))

    val jackson =
      sk.load.stream.jackson(jacksonPath, 100).mapFilter(_.value).compile.toList.unsafeRunSync().toSet
    assert(jackson == Set(en1, en2))

    val circe =
      sk.load.stream.circe(circePath, 100).mapFilter(_.value).compile.toList.unsafeRunSync().toSet
    assert(circe == Set(en1, en2))

  }

  test("sparKafka should be sent to kafka and save to multi avro") {
    val data = fs2
      .Stream(
        ProducerRecords(
          List(ProducerRecord(topicEnum.topicName.value, 0, en1), ProducerRecord(topicEnum.topicName.value, 1, en2))))
      .covary[IO]
      .through(topicEnum.fs2Channel.updateProducer(_.withClientId("kafka.avro.test3")).producerPipe)

    val path = "./data/test/spark/kafka/coproduct/multi-scalaenum.avro"
    val sk   = sparKafka.topic(topicEnum.topicDef)

    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicEnum.schemaRegistry.register >>
      data.compile.drain >>
      sk.fromKafka.flatMap(_.save.avro(path).folder.run) >>
      sk.load.rdd.avro(path).map(_.rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }

  test("should be sent to kafka and save to multi snappy avro") {
    val data = fs2
      .Stream(
        ProducerRecords(
          List(ProducerRecord(topicEnum.topicName.value, 0, en1), ProducerRecord(topicEnum.topicName.value, 1, en2))))
      .covary[IO]
      .through(topicEnum.fs2Channel.updateProducer(_.withClientId("kafka.avro.test4")).producerPipe)

    val path = "./data/test/spark/kafka/coproduct/multi-scalaenum.snappy.avro"
    val sk   = sparKafka.topic(topicEnum.topicDef)

    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicEnum.schemaRegistry.register >>
      data.compile.drain >>
      sk.fromKafka.flatMap(_.save.avro(path).snappy.folder.run) >>
      sk.load.rdd.avro(path).map(_.rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }
  test("should be sent to kafka and save to single snappy avro") {
    val data = fs2
      .Stream(
        ProducerRecords(
          List(ProducerRecord(topicEnum.topicName.value, 0, en1), ProducerRecord(topicEnum.topicName.value, 1, en2))))
      .covary[IO]
      .through(topicEnum.fs2Channel.updateProducer(_.withClientId("kafka.avro.test5")).producerPipe)
    val path = "./data/test/spark/kafka/coproduct/single-scalaenum.snappy.avro"
    val sk   = sparKafka.topic(topicEnum.topicDef)

    val run = topicEnum.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      topicEnum.schemaRegistry.register >>
      data.compile.drain >>
      sk.fromKafka.flatMap(_.save.avro(path).snappy.file.sink.compile.drain) >>
      sk.load.rdd.avro(path).map(_.rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }
}
