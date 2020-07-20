package mtest.spark.kafka

import java.time.{Instant, LocalDate, LocalDateTime}

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat.{Avro, CirceJson, Jackson, Parquet}
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.cats.implicits._
import fs2.kafka.ProducerRecord
import io.circe.generic.auto._
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object SaveTestData {
  final case class Chicken(a: Int, b: String, now: Instant, ldt: LocalDateTime, ld: LocalDate)

  val topic: KafkaTopic[IO, Int, Chicken] =
    TopicDef[Int, Chicken](TopicName("test.spark.kafka.save.load")).in(ctx)

}

class SaveTest extends AnyFunSuite {
  import SaveTestData._

  val sk: SparKafka[IO, Int, Chicken] = topic.sparKafka(range)

  val chickenPR: List[ProducerRecord[Int, Chicken]] =
    List.fill(100)(
      topic.fs2PR(
        Random.nextInt(),
        Chicken(Random.nextInt(), "aaa", Instant.now(), LocalDateTime.now, LocalDate.now)))
  val chickens: List[Chicken] = chickenPR.map(_.value)

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.send(chickenPR)).unsafeRunSync()

  test("sparKafka dump") {
    topic.sparKafka(range).dump.unsafeRunSync()
  }

  test("sparKafka single jackson") {
    val path = "./data/test/spark/kafka/single/jackson.json"
    val action = sk
      .withParamUpdate(_.withPathBuilder((_, _) => path))
      .fromKafka
      .flatMap(_.saveSingleJackson(blocker))
      .map(r => assert(r == 100))
    action.unsafeRunSync()
    val rst = sparkSession.jackson[OptionalKV[Int, Chicken]](path).collect().sorted

    assert(rst.flatMap(_.value).toList == chickens)
  }

  test("sparKafka single circe json") {
    val path = "./data/test/spark/kafka/single/circe.json"
    val action =
      sk.withParamUpdate(_.withPathBuilder((_, _) => path))
        .fromKafka
        .flatMap(_.saveSingleCirce(blocker))
        .map(r => assert(r == 100))
    action.unsafeRunSync()
    val rst = sparkSession.circe[OptionalKV[Int, Chicken]](path).collect().sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }

  test("sparKafka single avro") {
    val path = "./data/test/spark/kafka/single/data.avro"
    val action = sk
      .withParamUpdate(_.withPathBuilder((_, _) => path))
      .fromKafka
      .flatMap(_.saveSingleAvro(blocker))
      .map(r => assert(r == 100))
    action.unsafeRunSync()
    val rst: Array[OptionalKV[Int, Chicken]] =
      sparkSession.avro[OptionalKV[Int, Chicken]](path).collect().sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }

  test("sparKafka single parquet") {
    val path = "./data/test/spark/kafka/single/data.parquet"
    val action = sk
      .withParamUpdate(_.withPathBuilder((_, _) => path))
      .fromKafka
      .flatMap(_.saveSingleParquet(blocker))
      .map(r => assert(r == 100))
    action.unsafeRunSync()
    val rst = sparkSession.parquet[OptionalKV[Int, Chicken]](path).collect().sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }

  test("sparKafka multi parquet") {
    val path = "./data/test/spark/kafka/multi/parquet"
    val action =
      sk.fromKafka.map(_.toDF.repartition(3).write.mode(SaveMode.Overwrite).parquet(path))
    action.unsafeRunSync()
    val rst = topic.sparKafka.readParquet(path).rdd.collect().toList.sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }
  test("sparKafka multi json") {
    val path   = "./data/test/spark/kafka/multi/json"
    val action = sk.fromKafka.map(_.toDF.repartition(3).write.mode(SaveMode.Overwrite).json(path))
    action.unsafeRunSync()
    val rst = topic.sparKafka.readJson(path).rdd.collect().sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }
  test("sparKafka multi avro") {
    val path = "./data/test/spark/kafka/multi/avro"
    val action = sk.fromKafka.map(
      _.toDF.repartition(3).write.mode(SaveMode.Overwrite).format("avro").save(path))
    action.unsafeRunSync()
    val rst = topic.sparKafka.readAvro(path).rdd.collect().sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }
}
