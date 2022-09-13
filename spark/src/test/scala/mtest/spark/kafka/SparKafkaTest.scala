package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KUnknown, NJAvroCodec}
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.SchemaFor
import eu.timepit.refined.auto.*
import frameless.TypedDataset
import fs2.kafka.{ProducerRecord, ProducerRecords}
import mtest.spark.sparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Instant, LocalDate}
object SparKafkaTestData {
  final case class Duck(f: Int, g: String)

  final case class HasDuck(a: Int, b: String, c: LocalDate, d: Instant, e: Duck)
  val duck: Duck = Duck(0, "embeded")

  val data: HasDuck =
    HasDuck(0, "a", LocalDate.now, Instant.ofEpochMilli(Instant.now.toEpochMilli), duck)

  implicit val hasDuckEncoder: NJAvroCodec[HasDuck] = NJAvroCodec[HasDuck]
  implicit val intCodec: NJAvroCodec[Int]           = NJAvroCodec[Int]
  implicit val stringCodec: NJAvroCodec[String]     = NJAvroCodec[String]

  println(SchemaFor[HasDuck].schema)
}

class SparKafkaTest extends AnyFunSuite {
  import SparKafkaTestData.*
  implicit val ss: SparkSession = sparkSession

  val topic: KafkaTopic[IO, Int, HasDuck] = TopicDef[Int, HasDuck](TopicName("duck.test")).in(ctx)

  val loadData =
    fs2
      .Stream(ProducerRecords(
        List(ProducerRecord(topic.topicName.value, 1, data), ProducerRecord(topic.topicName.value, 2, data))))
      .covary[IO]
      .through(topic.produce.updateConfig(_.withClientId("spark.kafka.test")).pipe)
      .compile
      .drain

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt >> topic.schemaRegistry.register >> loadData)
    .unsafeRunSync()

  test("sparKafka read topic from kafka") {
    val rst = sparKafka.topic(topic.topicDef).fromKafka.map(_.rdd.collect()).unsafeRunSync()
    assert(rst.toList.flatMap(_.value) === List(data, data))
  }

  test("sparKafka read topic from kafka and show minutely aggragation result") {
    sparKafka.topic(topic.topicDef).fromKafka.map(_.stats.minutely.count()).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily-hour aggragation result") {
    sparKafka.topic(topic).fromKafka.map(_.stats.dailyHour.collect()).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily-minutes aggragation result") {
    sparKafka.topic(topic).fromKafka.map(_.stats.dailyMinute.count()).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily aggragation result") {
    sparKafka.topic(topic).fromKafka.map(_.stats.daily.count()).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show hourly aggragation result") {
    sparKafka.topic(topic).fromKafka.map(_.stats.hourly.count()).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show summary") {
    sparKafka.topic(topic).fromKafka.map(_.stats.summary).flatMap(IO.println).unsafeRunSync()
  }
  test("sparKafka should be able to bimap to other topic") {
    val src: KafkaTopic[IO, Int, Int]                = ctx.topic[Int, Int]("src.topic")
    val d1: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 1, 0, None, Some(1), "t", 0)
    val d2: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 2, 0, None, Some(2), "t", 0)
    val d3: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 3, 0, None, None, "t", 0)
    val d4: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 4, 0, None, Some(4), "t", 0)
    val ds: TypedDataset[NJConsumerRecord[Int, Int]] = TypedDataset.create(List(d1, d2, d3, d4))

    val birst =
      sparKafka
        .topic(src)
        .crRdd(ds.rdd)
        .bimap(_.toString, _ + 1)(NJAvroCodec[String], NJAvroCodec[Int])
        .rdd
        .collect()
        .toSet
    assert(birst.flatMap(_.value) == Set(2, 3, 5))
  }

  test("sparKafka should be able to flatmap to other topic") {
    val src: KafkaTopic[IO, Int, Int]                = ctx.topic[Int, Int]("src.topic")
    val d1: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 1, 0, None, Some(1), "t", 0)
    val d2: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 2, 0, None, Some(2), "t", 0)
    val d3: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 3, 0, None, None, "t", 0)
    val d4: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 4, 0, None, Some(4), "t", 0)
    val ds: TypedDataset[NJConsumerRecord[Int, Int]] = TypedDataset.create(List(d1, d2, d3, d4))

    val birst =
      sparKafka
        .topic(src)
        .crRdd(ds.rdd)
        .flatMap(m => m.value.map(x => NJConsumerRecord.value.set(Some(x - 1))(m)))(
          NJAvroCodec[Int],
          NJAvroCodec[Int])
        .rdd
        .collect()
        .toSet
    assert(birst.flatMap(_.value) == Set(0, 1, 3))
  }

  test("sparKafka someValue should filter out none values") {
    val cr1: NJConsumerRecord[Int, Int]              = NJConsumerRecord(0, 1, 0, None, Some(1), "t", 0)
    val cr2: NJConsumerRecord[Int, Int]              = NJConsumerRecord(0, 2, 0, Some(2), None, "t", 0)
    val cr3: NJConsumerRecord[Int, Int]              = NJConsumerRecord(0, 3, 0, Some(3), None, "t", 0)
    val crs: List[NJConsumerRecord[Int, Int]]        = List(cr1, cr2, cr3)
    val ds: TypedDataset[NJConsumerRecord[Int, Int]] = TypedDataset.create(crs)

    println(cr1.asJson.spaces2)
    println(
      cr1.toNJProducerRecord
        .modifyKey(_ + 1)
        .modifyValue(_ + 1)
        .withKey(1)
        .withValue(2)
        .withTimestamp(3)
        .withPartition(4)
        .asJson
        .spaces2)

    val t =
      sparKafka
        .topic[Int, Int]("some.value")
        .crRdd(ds.rdd)
        .repartition(3)
        .descendTimestamp
        .dismissNulls
        .transform(_.distinct())
    val rst = t.rdd.collect().flatMap(_.value)
    assert(rst === Seq(cr1.value.get))
  }

  test("should be able to save kunknown") {
    import io.circe.generic.auto.*
    val path = NJPath("./data/test/spark/kafka/kunknown")
    sparKafka
      .topic[Int, KUnknown]("duck.test")
      .fromKafka
      .flatMap(_.output.circe(path / "circe").run)
      .unsafeRunSync()
    sparKafka
      .topic[Int, KUnknown]("duck.test")
      .fromKafka
      .flatMap(_.output.jackson(path / "jackson").run)
      .unsafeRunSync()
    sparKafka
      .topic[Int, HasDuck]("duck.test")
      .fromKafka
      .flatMap(_.output.circe(path / "typed" / "circe").run)
      .unsafeRunSync()
    sparKafka
      .topic[Int, HasDuck]("duck.test")
      .fromKafka
      .flatMap(_.output.jackson(path / "typed" / "jackson").run)
      .unsafeRunSync()
  }
}
