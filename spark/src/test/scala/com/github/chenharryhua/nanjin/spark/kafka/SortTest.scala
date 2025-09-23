package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import eu.timepit.refined.auto.*
import mtest.spark.kafka.sparKafka
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class SortTest extends AnyFunSuite {
  val topic = AvroTopic[Int, Int](TopicName("topic"))

  val data = List(
    NJConsumerRecord[Int, Int]("topic", 0, 0, 40, 0, Nil, None, None, None, Some(0), Some(Random.nextInt())),
    NJConsumerRecord[Int, Int]("topic", 0, 1, 30, 0, Nil, None, None, None, Some(0), Some(Random.nextInt())),
    NJConsumerRecord[Int, Int]("topic", 0, 2, 20, 0, Nil, None, None, None, Some(0), Some(Random.nextInt())),
    NJConsumerRecord[Int, Int]("topic", 0, 3, 10, 0, Nil, None, None, None, Some(0), Some(Random.nextInt())),
    NJConsumerRecord[Int, Int]("topic", 1, 0, 40, 0, Nil, None, None, None, Some(1), Some(Random.nextInt())),
    NJConsumerRecord[Int, Int]("topic", 1, 1, 20, 0, Nil, None, None, None, Some(1), Some(Random.nextInt())),
    NJConsumerRecord[Int, Int]("topic", 1, 2, 20, 0, Nil, None, None, None, Some(1), Some(Random.nextInt())),
    NJConsumerRecord[Int, Int]("topic", 1, 4, 50, 0, Nil, None, None, None, Some(2), Some(Random.nextInt())),
    NJConsumerRecord[Int, Int](
      "topic",
      2,
      100,
      100,
      0,
      Nil,
      None,
      None,
      None,
      Some(2),
      Some(Random.nextInt())),
    NJConsumerRecord[Int, Int](
      "topic",
      2,
      100,
      100,
      0,
      Nil,
      None,
      None,
      None,
      Some(2),
      Some(Random.nextInt()))
  )
  val rdd = sparKafka.sparkSession.sparkContext.parallelize(data)
  val crRdd = sparKafka.topic(topic).crRdd(rdd)
  val prRdd = crRdd.prRdd

  test("produce record") {
    assert(
      prRdd.ascendTimestamp.rdd.collect().toList.map(_.key)
        === crRdd.ascendTimestamp.rdd.collect().toList.map(_.key))

    assert(
      prRdd.ascendOffset.rdd.collect().toList.map(_.key) == crRdd.ascendOffset.rdd.collect().toList.map(_.key)
    )
    assert(
      prRdd.descendTimestamp.rdd.collect().toList.map(_.key)
        === crRdd.descendTimestamp.rdd.collect().toList.map(_.key))

    assert(
      prRdd.descendOffset.rdd.collect().toList.map(_.key) == crRdd.descendOffset.rdd
        .collect()
        .toList
        .map(_.key)
    )
  }
  test("disorders") {
    assert(crRdd.stats[IO].disorders.map(_.count()).unsafeRunSync() == 4)
  }
  test("dup") {
    assert(crRdd.stats[IO].dupRecords.map(_.count()).unsafeRunSync() == 1)
  }
  test("missing offsets") {

    assert(crRdd.stats[IO].lostOffsets.map(_.count()).unsafeRunSync() == 1)
  }

  test("earliest") {
    assert(crRdd.stats[IO].lostEarliest.unsafeRunSync().size == 1)
  }

  test("latest") {
    assert(crRdd.stats[IO].lostLatest.unsafeRunSync().size == 1)
  }

}
