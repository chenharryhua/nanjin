package com.github.chenharryhua.nanjin.spark.kafka

import mtest.spark.{ctx, sparkSession}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class SortTest extends AnyFunSuite {
  val topic = ctx.topic[Int, Int]("topic")
  val ate   = OptionalKV.ate(topic.topicDef)

  val data = List(
    OptionalKV[Int, Int](0, 0, 40, Some(0), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](0, 1, 30, Some(1), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](0, 2, 20, Some(2), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](0, 3, 10, Some(3), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](1, 0, 40, Some(4), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](1, 1, 20, Some(5), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](1, 2, 20, Some(6), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](1, 4, 50, Some(7), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](2, 100, 100, Some(8), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](2, 100, 100, Some(9), Some(Random.nextInt()), "topic", 0)
  )
  val rdd   = sparkSession.sparkContext.parallelize(data)
  val crRdd = topic.sparKafka.crRdd(rdd)
  val crDS  = crRdd.crDS
  val prRdd = crRdd.prRdd

  test("offset") {
    val asRDD = crRdd.ascendOffset.rdd.collect.toList
    val asDS  = crDS.ascendOffset.dataset.collect.toList

    val dsRDD = crRdd.descendOffset.rdd.collect.toList
    val dsDS  = crDS.descendOffset.dataset.collect.toList

    assert(asRDD == asDS)
    assert(dsRDD == dsDS)
  }
  test("timestamp") {
    val asRDD = crRdd.ascendTimestamp.rdd.collect.toList
    val asDS  = crDS.ascendTimestamp.dataset.collect.toList

    val dsRDD = crRdd.descendTimestamp.rdd.collect.toList
    val dsDS  = crDS.descendTimestamp.dataset.collect.toList

    assert(asRDD == asDS)
    assert(dsRDD == dsDS)
  }

  test("produce record") {
    assert(
      prRdd.ascendTimestamp.rdd.collect().toList.map(_.key) == crRdd.ascendTimestamp.rdd
        .collect()
        .toList
        .map(_.key))

    assert(
      prRdd.ascendOffset.rdd.collect().toList.map(_.key) == crRdd.ascendOffset.rdd
        .collect()
        .toList
        .map(_.key))
    assert(
      prRdd.descendTimestamp.rdd.collect().toList.map(_.key) == crRdd.descendTimestamp.rdd
        .collect()
        .toList
        .map(_.key))

    assert(
      prRdd.descendOffset.rdd.collect().toList.map(_.key) == crRdd.descendOffset.rdd
        .collect()
        .toList
        .map(_.key))
  }
  test("disorders") {
    assert(crRdd.stats.disorders.dataset.count() == 4)
    crRdd.stats.summary.unsafeRunSync()
  }
  test("dup") {
    assert(crRdd.stats.dupRecords.dataset.count == 1)
  }
  test("missing offsets") {
    assert(crRdd.stats.missingOffsets.dataset.count == 1)
  }
}
