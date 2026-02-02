package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.messages.kafka.MetaInfo
import mtest.spark.sparkSession
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite

object StatisticsTestData {

  val dt: NJTimestamp = NJTimestamp("2012-10-26T18:00:00Z")
  val unit: Long = 1000 * 3600 * 24 // a day

  val list: List[MetaInfo] = List(
    MetaInfo("topic", 0, 0, dt.plus(unit * 1).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 0, 2, dt.plus(unit * 3).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 0, 3, dt.plus(unit * 4).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 0, 4, dt.plus(unit * 1).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 0, 5, dt.plus(unit * 6).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 0, 6, dt.plus(unit * 7).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 0, 7, dt.plus(unit * 8).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 0, 7, dt.plus(unit * 9).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 0, 7, dt.plus(unit * 9).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 1, 1, dt.plus(unit * 2).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 1, 2, dt.plus(unit * 3).milliseconds, 0.some, -1, -1),
    MetaInfo("topic", 1, 3, dt.plus(unit * 4).milliseconds, 0.some, -1, -1)
  )

  import sparkSession.implicits.*

  val ds: Dataset[MetaInfo] = sparkSession.createDataset(list)

  val empty: Dataset[MetaInfo] = sparkSession.emptyDataset[MetaInfo]

}

class StatisticsTest extends AnyFunSuite {
  import StatisticsTestData.*
  val stats = new Statistics[IO](ds)

  val emptyStats = new Statistics[IO](empty)

  test("dupRecords") {
    val res = stats.dupRecords.map(_.collect().toSet).unsafeRunSync()
    assert(res == Set(DuplicateRecord(0, 7, 3)))
    assert(emptyStats.dupRecords.map(_.count()).unsafeRunSync() == 0)
    stats.summary("sum").unsafeRunSync().foreach(x => println(x))
  }

  test("disorders") {
    val res = stats.disorders.map(_.collect().toSet).unsafeRunSync()
    assert(
      res == Set(Disorder(0, 3, 1351620000000L, "2012-10-31T05:00", "2012-10-28T05:00", 259200000L, Some(0))))

    assert(emptyStats.disorders.map(_.count()).unsafeRunSync() == 0)
  }

  test("missingOffsets") {
    val res = stats.lostOffsets.map(_.collect().toSet).unsafeRunSync()
    assert(res == Set(MissingOffset(0, 1)))
    assert(emptyStats.lostOffsets.map(_.count()).unsafeRunSync() == 0)
  }

  test("max/min") {
    assert(stats.minPartitionOffset.unsafeRunSync().value.map { case (tp, o) =>
      tp.partition() -> o
    } == Map(0 -> 0, 1 -> 1))
    assert(stats.maxPartitionOffset.unsafeRunSync().value.map { case (tp, o) =>
      tp.partition() -> o
    } == Map(0 -> 7, 1 -> 3))
  }

  test("emptyStats max/min") {
    assert(emptyStats.minPartitionOffset("min").unsafeRunSync().value == Map.empty)
    assert(emptyStats.maxPartitionOffset("max").unsafeRunSync().value == Map.empty)
  }
}
