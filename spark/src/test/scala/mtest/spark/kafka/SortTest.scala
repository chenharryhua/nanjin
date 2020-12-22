package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.kafka._
import org.scalatest.funsuite.AnyFunSuite
import mtest.spark.{contextShift, ctx, sparkSession, timer}
import org.apache.spark.rdd.RDD

import scala.util.Random

object SortTestData {

  val data: List[OptionalKV[Int, Int]] = List(
    OptionalKV[Int, Int](0, 0, 10000, Some(0), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](0, 1, 10001, Some(1), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](0, 2, 10002, Some(2), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](1, 10, 20000, Some(3), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](1, 11, 20001, Some(4), Some(Random.nextInt()), "topic", 0),
    OptionalKV[Int, Int](1, 12, 20002, Some(5), Some(Random.nextInt()), "topic", 0)
  ).sortBy(_.value.get)

  val rdd: RDD[OptionalKV[Int, Int]]  = sparkSession.sparkContext.parallelize(data)
  val topic: KafkaTopic[IO, Int, Int] = ctx.topic[Int, Int]("topic")
  val crRdd: CrRdd[IO, Int, Int]      = topic.sparKafka.crRdd(rdd)
  val crDs: CrDS[IO, Int, Int]        = crRdd.crDS
  val prRdd: PrRdd[IO, Int, Int]      = crRdd.prRdd

}

class SortTest extends AnyFunSuite {
  import SortTestData._
  test("rdd timestamp sort") {
    assert(crRdd.partitionOf(0).tsAscending.rdd.collect().toList.flatMap(_.key) == List(0, 1, 2))
    assert(crRdd.partitionOf(1).tsDescending.rdd.collect().toList.flatMap(_.key) == List(5, 4, 3))
  }
  test("ds timestamp sort") {
    assert(crDs.partitionOf(0).tsAscending.dataset.collect().toList.flatMap(_.key) == List(0, 1, 2))
    assert(
      crDs.partitionOf(1).tsDescending.dataset.collect().toList.flatMap(_.key) == List(5, 4, 3))
  }

  test("rdd offset sort") {
    assert(
      crRdd.partitionOf(0).offsetAscending.rdd.collect().toList.flatMap(_.key) == List(0, 1, 2))
    assert(
      crRdd.partitionOf(1).offsetDescending.rdd.collect().toList.flatMap(_.key) == List(5, 4, 3))
  }
  test("ds offset sort") {
    assert(
      crDs.partitionOf(0).offsetAscending.dataset.collect().toList.flatMap(_.key) == List(0, 1, 2))
    assert(
      crDs.partitionOf(1).offsetDescending.dataset.collect().toList.flatMap(_.key) == List(5, 4, 3))
  }
  test("pr sort") {
    assert(prRdd.ascending.rdd.collect.toList.flatMap(_.key) == List(0, 1, 2, 3, 4, 5))
  }
}
