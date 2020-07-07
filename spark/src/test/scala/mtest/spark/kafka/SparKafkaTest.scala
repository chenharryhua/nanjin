package mtest.spark.kafka

import java.time.{Instant, LocalDate}

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.kafka.codec.WithAvroSchema
import com.github.chenharryhua.nanjin.messages.kafka.{CompulsoryV, OptionalKV}
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.kafka._
import com.landoop.transportation.nyc.trip.yellow.trip_record
import frameless.TypedDataset
import frameless.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite

object SparKafkaTestData {
  final case class Duck(f: Int, g: String)

  final case class HasDuck(a: Int, b: String, c: LocalDate, d: Instant, e: Duck)
  val duck: Duck = Duck(0, "embeded")

  val data: HasDuck =
    HasDuck(0, "a", LocalDate.now, Instant.ofEpochMilli(Instant.now.toEpochMilli), duck)
}

class SparKafkaTest extends AnyFunSuite {
  import SparKafkaTestData._

  val topic: KafkaTopic[IO, Int, HasDuck] = TopicDef[Int, HasDuck](TopicName("duck.test")).in(ctx)

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >> topic.schemaRegister >>
    topic.send(List(topic.fs2PR(0, data), topic.fs2PR(1, data)))).unsafeRunSync()

  test("read topic from kafka") {
    val rst =
      topic.sparKafka(range).fromKafka.flatMap(_.crDataset.values.collect[IO]()).unsafeRunSync
    assert(rst.toList.map(_.value) === List(data, data))
  }

  test("read topic from kafka and show aggragation result") {
    topic.sparKafka(range).fromKafka.flatMap(_.stats.minutely).unsafeRunSync
  }

  test("should be able to bimap to other topic") {
    val src: KafkaTopic[IO, Int, Int]          = ctx.topic[Int, Int]("src.topic")
    val tgt: KafkaTopic[IO, String, Int]       = ctx.topic[String, Int]("target.topic")
    val d1: OptionalKV[Int, Int]               = OptionalKV(0, 1, 0, None, Some(1), "t", 0)
    val d2: OptionalKV[Int, Int]               = OptionalKV(0, 2, 0, None, Some(2), "t", 0)
    val d3: OptionalKV[Int, Int]               = OptionalKV(0, 3, 0, None, None, "t", 0)
    val d4: OptionalKV[Int, Int]               = OptionalKV(0, 4, 0, None, Some(4), "t", 0)
    val ds: TypedDataset[OptionalKV[Int, Int]] = TypedDataset.create(List(d1, d2, d3, d4))

    val birst: Set[CompulsoryV[String, Int]] =
      src
        .sparKafka(range)
        .crDataset(ds)
        .bimap(_.toString, _ + 1)
        .values
        .collect[IO]()
        .unsafeRunSync
        .toSet
    assert(birst.map(_.value) == Set(2, 3, 5))
  }

  test("should be able to flatmap to other topic") {
    val src: KafkaTopic[IO, Int, Int]          = ctx.topic[Int, Int]("src.topic")
    val tgt: KafkaTopic[IO, Int, Int]          = ctx.topic[Int, Int]("target.topic")
    val d1: OptionalKV[Int, Int]               = OptionalKV(0, 1, 0, None, Some(1), "t", 0)
    val d2: OptionalKV[Int, Int]               = OptionalKV(0, 2, 0, None, Some(2), "t", 0)
    val d3: OptionalKV[Int, Int]               = OptionalKV(0, 3, 0, None, None, "t", 0)
    val d4: OptionalKV[Int, Int]               = OptionalKV(0, 4, 0, None, Some(4), "t", 0)
    val ds: TypedDataset[OptionalKV[Int, Int]] = TypedDataset.create(List(d1, d2, d3, d4))

    val birst: Set[CompulsoryV[Int, Int]] =
      src
        .sparKafka(range)
        .crDataset(ds)
        .flatMap(m => m.value.map(x => OptionalKV.value.set(Some(x - 1))(m)))
        .values
        .collect[IO]()
        .unsafeRunSync
        .toSet
    assert(birst.map(_.value) == Set(0, 1, 3))
  }

  test("someValue should filter out none values") {
    val cr1: OptionalKV[Int, Int]              = OptionalKV(0, 1, 0, None, Some(1), "t", 0)
    val cr2: OptionalKV[Int, Int]              = OptionalKV(0, 2, 0, Some(2), None, "t", 0)
    val cr3: OptionalKV[Int, Int]              = OptionalKV(0, 3, 0, Some(3), None, "t", 0)
    val crs: List[OptionalKV[Int, Int]]        = List(cr1, cr2, cr3)
    val ds: TypedDataset[OptionalKV[Int, Int]] = TypedDataset.create(crs)

    val t   = TopicDef[Int, Int](TopicName("some.value")).in(ctx).sparKafka(range).crDataset(ds)
    val rst = t.values.collect[IO]().unsafeRunSync().map(_.value)
    assert(rst === Seq(cr1.value.get))
  }
}
