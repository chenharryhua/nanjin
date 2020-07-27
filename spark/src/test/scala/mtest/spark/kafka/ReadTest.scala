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

import scala.util.Random
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import cats.derived.auto.eq._
import cats.implicits._

object ReadTestData {
  final case class Dog(a: Int, b: String)

  val dogs: List[OptionalKV[Int, Dog]] = List
    .fill(100)(Dog(Random.nextInt(), "dog"))
    .mapWithIndex((d, i) => OptionalKV[Int, Dog](0, i.toLong, 0, Some(1), Some(d), "topic", 0))

  val dogs_noKey: List[OptionalKV[Int, Dog]] = List
    .fill(100)(Dog(Random.nextInt(), "dog"))
    .mapWithIndex((d, i) => OptionalKV[Int, Dog](0, i.toLong, 0, None, Some(d), "topic-nokey", 0))

  val topic: KafkaTopic[IO, Int, Dog] =
    TopicDef[Int, Dog](TopicName("test.spark.kafka.dogs")).in(ctx)

}

class ReadTest extends AnyFunSuite {
  import ReadTestData._

  test("sparKafka read parquet") {
    val data = TypedDataset.create(dogs_noKey)
    val path = "./data/test/spark/kafka/read/parquet"
    data.write.mode(SaveMode.Overwrite).parquet(path)
    val rst = topic.sparKafka.load.parquet(path)
    assert(rst.diff(data.dataset.rdd).count == 0)
  }

  test("sparKafka read avro") {
    val data = TypedDataset.create(dogs_noKey)
    val path = "./data/test/spark/kafka/read/avro"
    data.write.mode(SaveMode.Overwrite).format("avro").save(path)
    val rst = topic.sparKafka.load.avro(path)
    assert(rst.diff(data.dataset.rdd).count == 0)
  }

  test("sparKafka read parquet - compulsoryK") {
    val data: TypedDataset[CompulsoryK[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryK))
    val path = "./data/test/spark/kafka/read/parquet-compulsory"
    data.write.mode(SaveMode.Overwrite).parquet(path)
    val rst = topic.sparKafka(range).load.parquet(path)
    assert(rst.diff(data.deserialized.map(_.toOptionalKV).dataset.rdd).count == 0)
  }

  test("sparKafka read avro - compulsoryV") {
    val data: TypedDataset[CompulsoryV[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryV))
    val path = "./data/test/spark/kafka/read/avro-compulsory"
    data.write.mode(SaveMode.Overwrite).format("avro").save(path)
    val rst = topic.sparKafka(range).load.avro(path)
    assert(rst.diff(data.deserialized.map(_.toOptionalKV).dataset.rdd).count == 0)
  }

  test("sparKafka read json - compulsoryKV") {
    val data: TypedDataset[CompulsoryKV[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryKV))
    val path = "./data/test/spark/kafka/read/json-compulsory"
    data.write.mode(SaveMode.Overwrite).json(path)
    val rst = topic.sparKafka(range).load.circe(path)
    assert(rst.diff(data.dataset.rdd.map(_.toOptionalKV)).count == 0)
  }
}
