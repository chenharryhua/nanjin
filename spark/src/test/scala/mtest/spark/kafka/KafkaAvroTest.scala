package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import eu.timepit.refined.auto.*
import io.circe.Codec
import io.lemonlabs.uri.typesafe.dsl.*
import org.scalatest.funsuite.AnyFunSuite
import shapeless.*
import io.circe.generic.auto.*
import com.github.chenharryhua.nanjin.common.transformers.*
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

  val topicCO = AvroTopic[Int, PersonCaseObject](TopicName("test.spark.kafka.coproduct.caseobject"))

  val topicEnum = AvroTopic[Int, PersonEnum](TopicName("test.spark.kafka.coproduct.scalaenum"))

  val topicCoProd =
    AvroTopic[Int, PersonCoproduct](TopicName("test.spark.kafka.coproduct.shapelesscoproduct"))

}

class KafkaAvroTest extends AnyFunSuite {
  import KafkaAvroTestData.*

  test("sparKafka not work with case object -- task serializable issue(avro4s) - happy failure") {
    val data = fs2.Stream
      .emits(List((0, co1), (1, co2)))
      .covary[IO]
      .chunks
      .through(
        ctx.produce[Int, PersonCaseObject](topicCO).updateConfig(_.withClientId("kafka.avro.test1")).sink)
    val path = "./data/test/spark/kafka/coproduct/caseobject.avro"
    val sk = sparKafka.topic(topicCO)

    val run =
      ctx
        .admin(topicCO.topicName.name)
        .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
        ctx.schemaRegistry.register(topicCO) >>
        data.compile.drain >>
        sk.fromKafka.flatMap(_.output.avro(path).run[IO]) >>
        IO(sk.load.avro(path).rdd.collect().toSet)
    intercept[Exception](run.unsafeRunSync().flatMap(_.value) == Set(co1, co2))
  }

  test("sparKafka should be sent to kafka and save to single avro") {
    val data = fs2.Stream
      .emits(List((0, en1), (1, en2)))
      .covary[IO]
      .chunks
      .through(
        ctx.produce[Int, PersonEnum](topicEnum).updateConfig(_.withClientId("kafka.avro.test2")).sink)
    val avroPath = "./data/test/spark/kafka/coproduct/scalaenum.avro"
    val jacksonPath = "./data/test/spark/kafka/coproduct/scalaenum.jackson.json"
    val circePath = "./data/test/spark/kafka/coproduct/scalaenum.circe.json"
    val parquetPath = "./data/test/spark/kafka/coproduct/scalaenum.parquet"

    val sk = sparKafka.topic(topicEnum)

    val run =
      ctx
        .admin(topicEnum.topicName.name)
        .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
        ctx.schemaRegistry.register(topicEnum) >>
        data.compile.drain >>
        sk.fromKafka.flatMap(_.output.avro(avroPath).run[IO]) >>
        sk.fromKafka.flatMap(_.output.jackson(jacksonPath).run[IO]) >>
        sk.fromKafka.flatMap(_.output.circe(circePath).run[IO]) >>
        sk.fromKafka.flatMap(_.output.parquet(parquetPath).run[IO]) >>
        IO(sk.load.avro(avroPath).rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
    sparKafka.stats.jackson(jacksonPath).flatMap(_.summary).unsafeRunSync()
    sparKafka.stats.circe(circePath).flatMap(_.summary("sum")).unsafeRunSync()
    sparKafka.stats.avro(avroPath).flatMap(_.summary("sum")).unsafeRunSync()
    sparKafka.stats.parquet(parquetPath).flatMap(_.summary).unsafeRunSync()
  }

  test("sparKafka should be sent to kafka and save to multi avro") {
    val data = fs2.Stream
      .emits(List((0, en1), (1, en2)))
      .covary[IO]
      .chunks
      .through(
        ctx.produce[Int, PersonEnum](topicEnum).updateConfig(_.withClientId("kafka.avro.test3")).sink)

    val path = "./data/test/spark/kafka/coproduct/multi-scalaenum.avro"
    val sk = sparKafka.topic(topicEnum)

    val run =
      ctx
        .admin(topicEnum.topicName.name)
        .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
        ctx.schemaRegistry.register(topicEnum) >>
        data.compile.drain >>
        sk.fromKafka.flatMap(_.output.avro(path).run[IO]) >>
        IO(sk.load.avro(path).rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }

  test("should be sent to kafka and save to multi snappy avro") {
    val data = fs2.Stream
      .emits(List((0, en1), (1, en2)))
      .covary[IO]
      .chunks
      .through(
        ctx.produce[Int, PersonEnum](topicEnum).updateConfig(_.withClientId("kafka.avro.test4")).sink)

    val path = "./data/test/spark/kafka/coproduct/multi-scalaenum.snappy.avro"
    val sk = sparKafka.topic(topicEnum)

    val run =
      ctx
        .admin(topicEnum.topicName.name)
        .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
        ctx.schemaRegistry.register(topicEnum) >>
        data.compile.drain >>
        sk.fromKafka.flatMap(_.output.avro(path).withCompression(_.Snappy).run[IO]) >>
        IO(sk.load.avro(path).rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }
  test("should be sent to kafka and save to binary bzip2 avro") {
    val data = fs2.Stream
      .emits(List((0, en1), (1, en2)))
      .covary[IO]
      .chunks
      .through(
        ctx.produce[Int, PersonEnum](topicEnum).updateConfig(_.withClientId("kafka.avro.test5")).sink)
    val path = "./data/test/spark/kafka/coproduct/scalaenum.avro.bzip2"
    val sk = sparKafka.topic(topicEnum)

    val run =
      ctx
        .admin(topicEnum.topicName.name)
        .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
        ctx.schemaRegistry.register(topicEnum) >>
        data.compile.drain >>
        sk.fromKafka.flatMap(_.output.binAvro(path).withCompression(_.Bzip2).run[IO]) >>
        IO(sk.load.binAvro(path).rdd.take(10).toSet)
    assert(run.unsafeRunSync().flatMap(_.value) == Set(en1, en2))
  }

}
