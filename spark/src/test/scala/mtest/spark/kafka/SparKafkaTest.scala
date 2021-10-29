package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.sydneyTime
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.spark.kafka.*
import com.sksamuel.avro4s.SchemaFor
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

  implicit val hasDuckEncoder: AvroCodec[HasDuck] = AvroCodec[HasDuck]
  implicit val intCodec: AvroCodec[Int]           = AvroCodec[Int]
  implicit val stringCodec: AvroCodec[String]     = AvroCodec[String]

  println(SchemaFor[HasDuck].schema)
}

class SparKafkaTest extends AnyFunSuite {
  import SparKafkaTestData.*
  implicit val ss: SparkSession = sparkSession

  val topic: KafkaTopic[IO, Int, HasDuck] = TopicDef[Int, HasDuck](TopicName("duck.test")).in(ctx)

  val loadData =
    fs2
      .Stream(
        ProducerRecords(
          List(ProducerRecord(topic.topicName.value, 1, data), ProducerRecord(topic.topicName.value, 1, data))))
      .covary[IO]
      .through(topic.fs2Channel.updateProducer(_.withClientId("spark.kafka.test")).producerPipe)
      .compile
      .drain

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >> topic.schemaRegistry.register >> loadData)
    .unsafeRunSync()

  test("sparKafka read topic from kafka") {
    val rst = sparKafka.topic(topic.topicDef).fromKafka.map(_.rdd.collect()).unsafeRunSync()
    assert(rst.toList.flatMap(_.value) === List(data, data))
  }

  test("sparKafka read topic from kafka and show minutely aggragation result") {
    sparKafka
      .topic(topic.topicDef)
      .withOneDay(LocalDate.now())
      .fromKafka
      .flatMap(_.stats.rows(100).untruncate.truncate.minutely)
      .unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily-hour aggragation result") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.dailyHour).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily-minutes aggragation result") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.dailyMinute).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show daily aggragation result") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.daily).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show hourly aggragation result") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.hourly).unsafeRunSync()
  }
  test("sparKafka read topic from kafka and show summary") {
    sparKafka.topic(topic).fromKafka.flatMap(_.stats.summary).unsafeRunSync()
  }
  test("sparKafka should be able to bimap to other topic") {
    val src: KafkaTopic[IO, Int, Int]                = ctx.topic[Int, Int]("src.topic")
    val tgt: KafkaTopic[IO, String, Int]             = ctx.topic[String, Int]("target.topic")
    val d1: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 1, 0, None, Some(1), "t", 0)
    val d2: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 2, 0, None, Some(2), "t", 0)
    val d3: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 3, 0, None, None, "t", 0)
    val d4: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 4, 0, None, Some(4), "t", 0)
    val ds: TypedDataset[NJConsumerRecord[Int, Int]] = TypedDataset.create(List(d1, d2, d3, d4))

    val t = ctx.topic[String, Int]("tmp")

    val birst =
      sparKafka.topic(src).crRdd(ds.rdd).bimap(_.toString, _ + 1)(t).rdd.collect().toSet
    assert(birst.flatMap(_.value) == Set(2, 3, 5))
  }

  test("sparKafka should be able to flatmap to other topic") {
    val src: KafkaTopic[IO, Int, Int]                = ctx.topic[Int, Int]("src.topic")
    val tgt: KafkaTopic[IO, Int, Int]                = ctx.topic[Int, Int]("target.topic")
    val d1: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 1, 0, None, Some(1), "t", 0)
    val d2: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 2, 0, None, Some(2), "t", 0)
    val d3: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 3, 0, None, None, "t", 0)
    val d4: NJConsumerRecord[Int, Int]               = NJConsumerRecord(0, 4, 0, None, Some(4), "t", 0)
    val ds: TypedDataset[NJConsumerRecord[Int, Int]] = TypedDataset.create(List(d1, d2, d3, d4))

    val t = ctx.topic[Int, Int]("tmp")

    val birst =
      sparKafka
        .topic(src)
        .crRdd(ds.rdd)
        .timeRange
        .flatMap(m => m.value.map(x => NJConsumerRecord.value.set(Some(x - 1))(m)))(t)
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

    val t = sparKafka
      .topic[Int, Int]("some.value")
      .crRdd(ds.rdd)
      .repartition(3)
      .descendTimestamp
      .dismissNulls
      .transform(_.distinct())
    val rst = t.rdd.collect().flatMap(_.value)
    assert(rst === Seq(cr1.value.get))
    println(cr1.toString)
  }
}
