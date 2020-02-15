package mtest.spark.kafka

import cats.effect.IO
import cats.instances.all._
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.TypedDataset
import frameless.cats.implicits._
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

class LoadTopicDataFromDiskTest extends AnyFunSuite {

  val data: List[NJConsumerRecord[Int, Int]] = List(
    NJConsumerRecord(0, 0, 0, None, Some(0), "topic", 0),
    NJConsumerRecord(1, 1, 1, None, Some(1), "topic", 1),
    NJConsumerRecord(2, 2, 2, None, None, "topic", 2))

  val topic = ctx.topic[Int, Int]("load.topic.disk")

  test("load json test") {
    TypedDataset.create(data).write.mode(SaveMode.Overwrite).json("./data/test/load/json")
    val rst = topic.kit.sparKafka
      .withParamUpdate(_.withJson.withPathBuilder(_ => "./data/test/load/json"))
      .fromDisk[IO]
      .consumerRecords
      .flatMap(_.typedDataset.collect[IO].map(x => assert(x.sortBy(_.offset).toList === data)))
    rst.unsafeRunSync()
  }

  test("load avro test") {
    TypedDataset
      .create(data)
      .write
      .mode(SaveMode.Overwrite)
      .format("avro")
      .save("./data/test/load/avro")
    val rst = topic.kit.sparKafka
      .withParamUpdate(_.withAvro.withPathBuilder(_ => "./data/test/load/avro"))
      .fromDisk[IO]
      .consumerRecords
      .flatMap(_.typedDataset.collect[IO].map(x => assert(x.sortBy(_.offset).toList === data)))
    rst.unsafeRunSync()
  }

  test("load parquet test") {
    TypedDataset.create(data).write.mode(SaveMode.Overwrite).parquet("./data/test/load/parquet")
    val rst = topic.kit.sparKafka
      .withParamUpdate(_.withParquet.withPathBuilder(_ => "./data/test/load/parquet"))
      .fromDisk[IO]
      .consumerRecords
      .flatMap(_.typedDataset.collect[IO].map(x => assert(x.sortBy(_.offset).toList === data)))
    rst.unsafeRunSync()
  }
}
