package mtest.spark.kafka

import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, ZoneId, ZonedDateTime}

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.cats.implicits._
import fs2.kafka.ProducerRecord
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite
import cats.derived.auto.show._

import scala.util.Random

object SaveTestData {
  implicit val zoneId: ZoneId = sydneyTime

  final case class Chicken(a: Int, b: String, now: Instant, ldt: LocalDateTime, ld: LocalDate)

  val topic: KafkaTopic[IO, Int, Chicken] =
    TopicDef[Int, Chicken](TopicName("test.spark.kafka.save.load")).in(ctx)

}

class SaveDateTimeTest extends AnyFunSuite {
  import SaveTestData._

  def sk(path: String): SparKafka[IO, Int, Chicken] =
    topic.sparKafka(range)

  val chickenPR: List[ProducerRecord[Int, Chicken]] =
    List.fill(100)(
      topic.fs2PR(
        Random.nextInt(),
        Chicken(Random.nextInt(), "aaa", Instant.now(), LocalDateTime.now, LocalDate.now)))
  val chickens: List[Chicken] = chickenPR.map(_.value)

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.send(chickenPR)).unsafeRunSync()

  test("sparKafka datetime dump") {
    sk("doesn't matter").dump.unsafeRunSync()
  }

  test("sparKafka datetime single jackson") {
    val path = "./data/test/spark/kafka/single/jackson.json"

    val action = sk(path).fromKafka.flatMap(_.save.jackson(path).single.run(blocker))
    action.unsafeRunSync()
    val rst = sparkSession.load.jackson[OptionalKV[Int, Chicken]](path).collect().sorted

    assert(rst.flatMap(_.value).toList == chickens)
  }

  test("sparKafka datetime single circe json") {
    val path = "./data/test/spark/kafka/single/circe.json"
    val action =
      sk(path).fromKafka.flatMap(_.rdd.save[IO].circe(path).single.run(blocker))
    action.unsafeRunSync()
    val rst = sparkSession.load.circe[OptionalKV[Int, Chicken]](path).collect().sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }

  test("sparKafka datetime single avro") {
    val path   = "./data/test/spark/kafka/single/data.avro"
    val action = sk(path).fromKafka.flatMap(_.save.avro(path).single.run(blocker))
    action.unsafeRunSync()
    val rst: Array[OptionalKV[Int, Chicken]] =
      sparkSession.load.avro[OptionalKV[Int, Chicken]](path).collect().sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }

  test("sparKafka datetime multi avro") {
    val path   = "./data/test/spark/kafka/multi/data.avro"
    val action = sk(path).fromKafka.flatMap(_.save.avro(path).multi.run(blocker))
    action.unsafeRunSync()
    val rst: Array[OptionalKV[Int, Chicken]] =
      sparkSession.load.avro[OptionalKV[Int, Chicken]](path).collect().sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }

  test("sparKafka datetime multi jackson") {
    val path   = "./data/test/spark/kafka/multi/jackson.json"
    val action = sk(path).fromKafka.flatMap(_.save.jackson(path).multi.run(blocker))
    action.unsafeRunSync()
    val rst: Array[OptionalKV[Int, Chicken]] =
      sparkSession.load.jackson[OptionalKV[Int, Chicken]](path).collect().sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }

  test("partition avro") {
    topic.sparKafka.fromKafka.flatMap(_.save.partitionAvro.run(blocker)).unsafeRunSync()
  }
  test("partition binary avro") {
    topic.sparKafka.fromKafka.flatMap(_.save.partitionBinAvro.run(blocker)).unsafeRunSync()
  }
  test("partition jackson") {
    topic.sparKafka.fromKafka.flatMap(_.save.partitionJackson.run(blocker)).unsafeRunSync()
  }
  test("partition parquet") {
    topic.sparKafka.fromKafka.flatMap(_.save.partitionParquet.run(blocker)).unsafeRunSync()
  }
  test("partition circe") {
    topic.sparKafka.fromKafka.flatMap(_.save.partitionCirce.run(blocker)).unsafeRunSync()
  }
  test("partition java object") {
    topic.sparKafka.fromKafka.flatMap(_.save.partitionJavaObject.run(blocker)).unsafeRunSync()
  }
  test("partition text") {
    topic.sparKafka.fromKafka.flatMap(_.save.partitionText.run(blocker)).unsafeRunSync()
  }
}
