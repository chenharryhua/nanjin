package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.{CompulsoryKV, OptionalKV}
import frameless.TypedDataset
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark.kafka._

import scala.util.Random
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._

object ReadTestData {
  final case class Dog(a: Int, b: String)

  val dogs: List[OptionalKV[Int, Dog]] = List
    .fill(100)(Dog(Random.nextInt(), "dog"))
    .map(d => OptionalKV[Int, Dog](0, 0, 0, Some(1), Some(d), "tp", 0))

  val topic: KafkaTopic[IO, Int, Dog] =
    TopicDef[Int, Dog](TopicName("test.spark.kafka.dogs")).in(ctx)

}

class ReadTest extends AnyFunSuite {
  import ReadTestData._

  test("read parquet") {
    val data = TypedDataset.create(dogs)
    val path = "./data/test/spark/kafka/read/parquet"
    data.write.mode(SaveMode.Overwrite).parquet(path)
    val rst = topic.sparKafka.readParquet(path).typedDataset.collect[IO]().unsafeRunSync().toSet
    assert(rst === dogs.toSet)
  }

  test("read avro") {
    val data = TypedDataset.create(dogs)
    val path = "./data/test/spark/kafka/read/avro"
    data.write.mode(SaveMode.Overwrite).format("avro").save(path)
    val rst = topic.sparKafka.readAvro(path).typedDataset.collect[IO]().unsafeRunSync().toSet
    assert(rst === dogs.toSet)
  }

  test("read json") {
    val data = TypedDataset.create(dogs)
    val path = "./data/test/spark/kafka/read/json"
    data.write.mode(SaveMode.Overwrite).json(path)
    val rst =
      topic.sparKafka.readJson(path).crDataset.typedDataset.collect[IO]().unsafeRunSync().toSet
    assert(rst === dogs.toSet)
  }

  test("read parquet - compulsory") {
    val data: TypedDataset[CompulsoryKV[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryKV))
    val path = "./data/test/spark/kafka/read/parquet-compulsory"
    data.write.mode(SaveMode.Overwrite).parquet(path)
    val rst = topic.sparKafka.readParquet(path).typedDataset.collect[IO]().unsafeRunSync().toSet
    assert(rst === dogs.toSet)
  }

  test("read avro - compulsory") {
    val data: TypedDataset[CompulsoryKV[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryKV))
    val path = "./data/test/spark/kafka/read/avro-compulsory"
    data.write.mode(SaveMode.Overwrite).format("avro").save(path)
    val rst = topic.sparKafka.readAvro(path).typedDataset.collect[IO]().unsafeRunSync().toSet
    assert(rst === dogs.toSet)
  }

  test("read json - compulsory") {
    val data: TypedDataset[CompulsoryKV[Int, Dog]] =
      TypedDataset.create(dogs.flatMap(_.toCompulsoryKV))
    val path = "./data/test/spark/kafka/read/json-compulsory"
    data.write.mode(SaveMode.Overwrite).json(path)
    val rst =
      topic.sparKafka.readJson(path).crDataset.typedDataset.collect[IO]().unsafeRunSync().toSet
    assert(rst === dogs.toSet)
  }
}
