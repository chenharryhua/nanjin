package mtest.spark.kafka

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.spark.kafka.{CompulsoryKV, _}
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder}
import io.circe.generic.auto._
import mtest.spark.{blocker, contextShift, ctx, sparkSession}
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object ReadTestData {
  final case class Dog(a: Int, b: String)

  implicit val te: TypedEncoder[Dog] = shapeless.cachedImplicit

  val dogs: List[OptionalKV[Int, Dog]] = List
    .fill(100)(Dog(Random.nextInt(), "dog"))
    .mapWithIndex((d, i) => OptionalKV[Int, Dog](0, i.toLong, 0, Some(1), Some(d), "topic", 0))

  val dogs_noKey: List[OptionalKV[Int, Dog]] = List
    .fill(100)(Dog(Random.nextInt(), "dog"))
    .mapWithIndex((d, i) => OptionalKV[Int, Dog](0, i.toLong, 0, None, Some(d), "topic-nokey", 0))

  val topic: SparKafka[IO, Int, Dog] =
    ctx
      .topic[Int, Dog]("to.be.rename")
      .sparKafka
      .withTopicName("test.spark.kafka.dogs")

}

class ReadTest extends AnyFunSuite {
  import ReadTestData._

  test("sparKafka read parquet") {
    val data = TypedDataset.create(dogs_noKey)
    val path = "./data/test/spark/kafka/read/parquet"
    data.write.mode(SaveMode.Overwrite).parquet(path)
    assert(topic.load.parquet(path).dataset.collect.toSet == dogs_noKey.toSet)
  }

  test("sparKafka read avro") {
    val data = TypedDataset.create(dogs_noKey)
    val path = "./data/test/spark/kafka/read/avro"
    data.write.mode(SaveMode.Overwrite).format("avro").save(path)
    assert(topic.load.avro(path).dataset.collect.toSet == dogs_noKey.toSet)
    assert(topic.load.rdd.avro(path).rdd.collect.toSet == dogs_noKey.toSet)
  }

  test("sparKafka read parquet - compulsoryK") {
    val data: TypedDataset[CompulsoryK[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryK))
    val path = "./data/test/spark/kafka/read/parquet-compulsory"
    data.write.mode(SaveMode.Overwrite).parquet(path)
    assert(topic.load.parquet(path).dataset.collect.toSet == dogs.toSet)
  }

  test("sparKafka read avro - compulsoryV") {
    val data: TypedDataset[CompulsoryV[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryV))
    val path = "./data/test/spark/kafka/read/avro-compulsory"
    data.write.mode(SaveMode.Overwrite).format("avro").save(path)
    assert(topic.load.avro(path).dataset.collect.toSet == dogs.toSet)
    assert(topic.load.rdd.avro(path).rdd.collect.toSet == dogs.toSet)
  }

  test("sparKafka read json - compulsoryKV") {
    val data: TypedDataset[CompulsoryKV[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryKV))
    val path = "./data/test/spark/kafka/read/json-compulsory"
    data.write.mode(SaveMode.Overwrite).json(path)
    assert(topic.load.json(path).dataset.collect.toSet == dogs.toSet)
    assert(topic.load.circe(path).dataset.collect.toSet == dogs.toSet)
  }
  test("sparKafka save producer records") {
    val path = "./data/test/spark/kafka/pr/json"
    topic
      .crRdd(sparkSession.sparkContext.parallelize(dogs))
      .prRdd
      .ascendTimestamp
      .save
      .circe(path)
      .run(blocker)
      .unsafeRunSync()
  }
}
