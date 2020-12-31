package mtest.spark.kafka

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.sydneyTime
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.kafka.{CompulsoryV, _}
import com.sksamuel.avro4s.SchemaFor
import frameless.TypedDataset
import frameless.cats.implicits._
import mtest.spark.{contextShift, ctx, sparKafka, sparkSession, timer}
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Instant, LocalDate}
import org.apache.spark.sql.SparkSession

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
  import SparKafkaTestData._
  implicit val ss: SparkSession = sparkSession

  val topic: KafkaTopic[IO, Int, HasDuck] = TopicDef[Int, HasDuck](TopicName("duck.test")).in(ctx)

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >> topic.schemaRegister >>
    topic.send(List(topic.fs2PR(0, data), topic.fs2PR(1, data)))).unsafeRunSync()

  test("sparKafka read topic from kafka") {
    val rst =
      sparKafka.topic(topic.topicDef).withTimeRange(range).fromKafka.values.collect()
    assert(rst.toList.map(_.value) === List(data, data))
  }

  test("sparKafka read topic from kafka and show minutely aggragation result") {
    sparKafka
      .topic(topic.topicDef)
      .withZoneId(sydneyTime)
      .fromKafka
      .stats
      .rows(100)
      .untruncate
      .truncate
      .minutely
      .unsafeRunSync
  }
  test("sparKafka read topic from kafka and show daily-hour aggragation result") {
    sparKafka.topic(topic).withTimeRange(range).fromKafka.stats.dailyHour.unsafeRunSync
  }
  test("sparKafka read topic from kafka and show daily-minutes aggragation result") {
    sparKafka.topic(topic).withTimeRange(range).fromKafka.stats.dailyMinute.unsafeRunSync
  }
  test("sparKafka read topic from kafka and show daily aggragation result") {
    sparKafka.topic(topic).withTimeRange(range).fromKafka.stats.daily.unsafeRunSync
  }
  test("sparKafka read topic from kafka and show hourly aggragation result") {
    sparKafka.topic(topic).withTimeRange(range).fromKafka.stats.hourly.unsafeRunSync
  }
  test("sparKafka read topic from kafka and show summary") {
    sparKafka.topic(topic).withTimeRange(range).fromKafka.stats.showSummary.unsafeRunSync
  }
  test("sparKafka should be able to bimap to other topic") {
    val src: KafkaTopic[IO, Int, Int]          = ctx.topic[Int, Int]("src.topic")
    val tgt: KafkaTopic[IO, String, Int]       = ctx.topic[String, Int]("target.topic")
    val d1: OptionalKV[Int, Int]               = OptionalKV(0, 1, 0, None, Some(1), "t", 0)
    val d2: OptionalKV[Int, Int]               = OptionalKV(0, 2, 0, None, Some(2), "t", 0)
    val d3: OptionalKV[Int, Int]               = OptionalKV(0, 3, 0, None, None, "t", 0)
    val d4: OptionalKV[Int, Int]               = OptionalKV(0, 4, 0, None, Some(4), "t", 0)
    val ds: TypedDataset[OptionalKV[Int, Int]] = TypedDataset.create(List(d1, d2, d3, d4))

    val t = ctx.topic[String, Int]("tmp")

    val birst: Set[CompulsoryV[String, Int]] =
      sparKafka.topic(src).withTimeRange(range).crRdd(ds.rdd).bimap(_.toString, _ + 1)(t).values.collect().toSet
    assert(birst.map(_.value) == Set(2, 3, 5))
  }

  test("sparKafka should be able to flatmap to other topic") {
    val src: KafkaTopic[IO, Int, Int]          = ctx.topic[Int, Int]("src.topic")
    val tgt: KafkaTopic[IO, Int, Int]          = ctx.topic[Int, Int]("target.topic")
    val d1: OptionalKV[Int, Int]               = OptionalKV(0, 1, 0, None, Some(1), "t", 0)
    val d2: OptionalKV[Int, Int]               = OptionalKV(0, 2, 0, None, Some(2), "t", 0)
    val d3: OptionalKV[Int, Int]               = OptionalKV(0, 3, 0, None, None, "t", 0)
    val d4: OptionalKV[Int, Int]               = OptionalKV(0, 4, 0, None, Some(4), "t", 0)
    val ds: TypedDataset[OptionalKV[Int, Int]] = TypedDataset.create(List(d1, d2, d3, d4))

    val t = ctx.topic[Int, Int]("tmp")

    val birst: Set[CompulsoryV[Int, Int]] =
      sparKafka
        .topic(src)
        .withTimeRange(range)
        .crRdd(ds.rdd)
        .timeRange
        .flatMap(m => m.value.map(x => OptionalKV.value.set(Some(x - 1))(m)))(t)
        .values
        .collect()
        .toSet
    assert(birst.map(_.value) == Set(0, 1, 3))
  }

  test("sparKafka someValue should filter out none values") {
    val cr1: OptionalKV[Int, Int]              = OptionalKV(0, 1, 0, None, Some(1), "t", 0)
    val cr2: OptionalKV[Int, Int]              = OptionalKV(0, 2, 0, Some(2), None, "t", 0)
    val cr3: OptionalKV[Int, Int]              = OptionalKV(0, 3, 0, Some(3), None, "t", 0)
    val crs: List[OptionalKV[Int, Int]]        = List(cr1, cr2, cr3)
    val ds: TypedDataset[OptionalKV[Int, Int]] = TypedDataset.create(crs)

    val t = sparKafka
      .topic[Int, Int]("some.value")
      .withTimeRange(range)
      .crRdd(ds.rdd)
      .repartition(3)
      .descendTimestamp
      .dismissNulls
      .transform(_.distinct)
    val rst = t.values.collect().map(_.value)
    assert(rst === Seq(cr1.value.get))
    println(cr1.show)
  }
}
