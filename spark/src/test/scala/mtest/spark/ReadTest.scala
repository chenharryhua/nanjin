package mtest.spark

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import frameless.{TypedDataset, TypedEncoder}
import io.circe.generic.auto._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object ReadTestData {
  final case class Dog(a: Int, b: String)

  implicit val te: TypedEncoder[Dog] = shapeless.cachedImplicit

  val dogs: List[NJConsumerRecord[Int, Dog]] = List
    .fill(100)(Dog(Random.nextInt(), "dog"))
    .mapWithIndex((d, i) => NJConsumerRecord[Int, Dog](0, i.toLong, 0, Some(1), Some(d), "topic", 0))

  val dogs_noKey: List[NJConsumerRecord[Int, Dog]] = List
    .fill(100)(Dog(Random.nextInt(), "dog"))
    .mapWithIndex((d, i) => NJConsumerRecord[Int, Dog](0, i.toLong, 0, None, Some(d), "topic-nokey", 0))

  val topic: SparKafkaTopic[IO, Int, Dog] = sparKafka.topic[Int, Dog]("test.spark.kafka.dogs")

}

class ReadTest extends AnyFunSuite {
  import ReadTestData._
  implicit val ss: SparkSession = sparkSession

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
