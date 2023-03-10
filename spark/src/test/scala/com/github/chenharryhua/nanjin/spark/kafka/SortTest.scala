package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkSessionExt}
import mtest.spark.kafka.sparKafka
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class SortTest extends AnyFunSuite {
  val topic = TopicDef[Int, Int](TopicName("topic"))
  val ate   = AvroTypedEncoder(topic)

  val data = List(
    NJConsumerRecord[Int, Int](0, 0, 40, Some(0), Some(Random.nextInt()), "topic", 0, Nil),
    NJConsumerRecord[Int, Int](0, 1, 30, Some(0), Some(Random.nextInt()), "topic", 0, Nil),
    NJConsumerRecord[Int, Int](0, 2, 20, Some(0), Some(Random.nextInt()), "topic", 0, Nil),
    NJConsumerRecord[Int, Int](0, 3, 10, Some(0), Some(Random.nextInt()), "topic", 0, Nil),
    NJConsumerRecord[Int, Int](1, 0, 40, Some(1), Some(Random.nextInt()), "topic", 0, Nil),
    NJConsumerRecord[Int, Int](1, 1, 20, Some(1), Some(Random.nextInt()), "topic", 0, Nil),
    NJConsumerRecord[Int, Int](1, 2, 20, Some(1), Some(Random.nextInt()), "topic", 0, Nil),
    NJConsumerRecord[Int, Int](1, 4, 50, Some(2), Some(Random.nextInt()), "topic", 0, Nil),
    NJConsumerRecord[Int, Int](2, 100, 100, Some(2), Some(Random.nextInt()), "topic", 0, Nil),
    NJConsumerRecord[Int, Int](2, 100, 100, Some(2), Some(Random.nextInt()), "topic", 0, Nil)
  )
  val rdd   = sparKafka.sparkSession.sparkContext.parallelize(data)
  val crRdd = sparKafka.topic(topic).crRdd(IO(rdd))
  val prRdd = crRdd.prRdd

  val njDataset = sparKafka.sparkSession.loadTable[IO](ate).data(rdd).fdataset

  test("produce record") {
    assert(
      prRdd.ascendTimestamp.frdd
        .map(_.collect().toList.map(_.key))
        .unsafeRunSync() == crRdd.ascendTimestamp.frdd.map(_.collect().toList.map(_.key)).unsafeRunSync())

    assert(
      prRdd.ascendOffset.frdd.map(_.collect().toList.map(_.key)).unsafeRunSync() == crRdd.ascendOffset.frdd
        .map(_.collect().toList.map(_.key))
        .unsafeRunSync())
    assert(
      prRdd.descendTimestamp.frdd
        .map(_.collect().toList.map(_.key))
        .unsafeRunSync() == crRdd.descendTimestamp.frdd.map(_.collect().toList.map(_.key)).unsafeRunSync())

    assert(
      prRdd.descendOffset.frdd.map(_.collect().toList.map(_.key)).unsafeRunSync() == crRdd.descendOffset.frdd
        .map(_.collect().toList.map(_.key))
        .unsafeRunSync())
  }
  test("disorders") {
    assert(crRdd.stats.disorders.map(_.count()).unsafeRunSync() == 4)
  }
  test("dup") {
    assert(crRdd.stats.dupRecords.map(_.count()).unsafeRunSync() == 1)
  }
  test("missing offsets") {

    assert(crRdd.stats.missingOffsets.map(_.count()).unsafeRunSync() == 1)
  }

  test("misorder keys") {
    import com.github.chenharryhua.nanjin.spark.kafka.functions.NJConsumerRecordDatasetExt
    assert(njDataset.misorderedKey.unsafeRunSync().count() == 4)
  }

  test("misplaced keys") {
    import com.github.chenharryhua.nanjin.spark.kafka.functions.NJConsumerRecordDatasetExt
    assert(njDataset.misplacedKey.unsafeRunSync().count() == 1)
  }
}
