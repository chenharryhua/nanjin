package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.TypedDataset
import org.scalatest.funsuite.AnyFunSuite
import cats.instances.all._
import com.github.chenharryhua.nanjin.kafka.common.NJConsumerRecord
import org.apache.spark.sql.SaveMode
import frameless.cats.implicits._

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
      .typedDataset
      .collect[IO]
      .map(x => assert(x.sortBy(_.offset).toList === data))
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
      .typedDataset
      .collect[IO]
      .map(x => assert(x.sortBy(_.offset).toList === data))
    rst.unsafeRunSync()
  }

  test("load parquet test") {
    TypedDataset.create(data).write.mode(SaveMode.Overwrite).parquet("./data/test/load/parquet")
    val rst = topic.kit.sparKafka
      .withParamUpdate(_.withParquet.withPathBuilder(_ => "./data/test/load/parquet"))
      .fromDisk[IO]
      .typedDataset
      .collect[IO]
      .map(x => assert(x.sortBy(_.offset).toList === data))
    rst.unsafeRunSync()
  }
}
