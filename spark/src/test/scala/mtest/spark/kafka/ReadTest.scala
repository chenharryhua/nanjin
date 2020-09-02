package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.{
  CompulsoryK,
  CompulsoryKV,
  CompulsoryV,
  OptionalKV
}
import frameless.TypedDataset
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark.kafka._
import com.github.chenharryhua.nanjin.spark._
import scala.util.Random
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import cats.derived.auto.eq._
import cats.implicits._
import frameless.TypedEncoder

object ReadTestData {
  final case class Dog(a: Int, b: String)

  implicit val te: TypedEncoder[Dog] = shapeless.cachedImplicit

  val dogs: List[OptionalKV[Int, Dog]] = List
    .fill(100)(Dog(Random.nextInt(), "dog"))
    .mapWithIndex((d, i) => OptionalKV[Int, Dog](0, i.toLong, 0, Some(1), Some(d), "topic", 0))

  val dogs_noKey: List[OptionalKV[Int, Dog]] = List
    .fill(100)(Dog(Random.nextInt(), "dog"))
    .mapWithIndex((d, i) => OptionalKV[Int, Dog](0, i.toLong, 0, None, Some(d), "topic-nokey", 0))

  val topic: TopicDef[Int, Dog] =
    TopicDef[Int, Dog](TopicName("test.spark.kafka.dogs"))

}

class ReadTest extends AnyFunSuite {
  import ReadTestData._

  val tp = sparkSession.alongWith[IO](ctx).topic(topic)

  test("sparKafka read parquet") {
    val data = TypedDataset.create(dogs_noKey)
    val path = "./data/test/spark/kafka/read/parquet"
    data.write.mode(SaveMode.Overwrite).parquet(path)
    assert(tp.load.tds.parquet(path).dataset.collect.toSet == dogs_noKey.toSet)
    assert(tp.load.rdd.parquet(path).rdd.collect.toSet == dogs_noKey.toSet)
  }

  test("sparKafka read avro") {
    val data = TypedDataset.create(dogs_noKey)
    val path = "./data/test/spark/kafka/read/avro"
    data.write.mode(SaveMode.Overwrite).format("avro").save(path)
    assert(tp.load.tds.avro(path).dataset.collect.toSet == dogs_noKey.toSet)
    assert(tp.load.rdd.avro(path).rdd.collect.toSet == dogs_noKey.toSet)
  }

  test("sparKafka read parquet - compulsoryK") {
    val data: TypedDataset[CompulsoryK[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryK))
    val path = "./data/test/spark/kafka/read/parquet-compulsory"
    data.write.mode(SaveMode.Overwrite).parquet(path)
    assert(tp.load.tds.parquet(path).dataset.collect.toSet == dogs.toSet)
    assert(tp.load.rdd.parquet(path).rdd.collect.toSet == dogs.toSet)
  }

  test("sparKafka read avro - compulsoryV") {
    val data: TypedDataset[CompulsoryV[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryV))
    val path = "./data/test/spark/kafka/read/avro-compulsory"
    data.write.mode(SaveMode.Overwrite).format("avro").save(path)
    assert(tp.load.tds.avro(path).dataset.collect.toSet == dogs.toSet)
    assert(tp.load.rdd.avro(path).rdd.collect.toSet == dogs.toSet)
  }

  test("sparKafka read json - compulsoryKV") {
    val data: TypedDataset[CompulsoryKV[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryKV))
    val path = "./data/test/spark/kafka/read/json-compulsory"
    data.write.mode(SaveMode.Overwrite).json(path)
    assert(tp.load.tds.json(path).dataset.collect.toSet == dogs.toSet)
  }
}
