package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import mtest.spark.sparkSession
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite

object StatisticsTestData {

  val dt: NJTimestamp = NJTimestamp("2012-10-26T18:00:00Z")
  val unit: Long      = 1000 * 3600 * 24 // a day

  val list = List(
    CRMetaInfo("topic", 0, 0, dt.plus(unit * 1).milliseconds, 0),
    CRMetaInfo("topic", 0, 2, dt.plus(unit * 3).milliseconds, 0),
    CRMetaInfo("topic", 0, 3, dt.plus(unit * 4).milliseconds, 0),
    CRMetaInfo("topic", 0, 4, dt.plus(unit * 1).milliseconds, 0),
    CRMetaInfo("topic", 0, 5, dt.plus(unit * 6).milliseconds, 0),
    CRMetaInfo("topic", 0, 6, dt.plus(unit * 7).milliseconds, 0),
    CRMetaInfo("topic", 0, 7, dt.plus(unit * 8).milliseconds, 0),
    CRMetaInfo("topic", 0, 7, dt.plus(unit * 9).milliseconds, 0),
    CRMetaInfo("topic", 0, 7, dt.plus(unit * 9).milliseconds, 0),
    CRMetaInfo("topic", 1, 1, dt.plus(unit * 2).milliseconds, 0),
    CRMetaInfo("topic", 1, 2, dt.plus(unit * 3).milliseconds, 0),
    CRMetaInfo("topic", 1, 3, dt.plus(unit * 4).milliseconds, 0)
  )
  import sparkSession.implicits.*

  val ds: Dataset[CRMetaInfo] = sparkSession.createDataset(list)

  val empty: Dataset[CRMetaInfo] = sparkSession.emptyDataset[CRMetaInfo]

}

class StatisticsTest extends AnyFunSuite {
  import StatisticsTestData.*
  val stats = new Statistics(ds, sydneyTime)

  val emptyStats = new Statistics(empty, sydneyTime)

  test("dupRecords") {
    val res = stats.dupRecords.collect().toSet
    assert(res == Set(DuplicateRecord(0, 7, 3)))
    assert(emptyStats.dupRecords.count() == 0)
  }

  test("disorders") {
    val res = stats.disorders.collect().toSet
    assert(res == Set(Disorder(0, 3, 1351620000000L, "2012-10-31T05:00", "2012-10-28T05:00", 259200000L, 0)))

    assert(emptyStats.disorders.count() == 0)
  }

  test("missingOffsets") {
    val res = stats.missingOffsets.collect().toSet
    assert(res == Set(MissingOffset(0, 1)))
    assert(emptyStats.missingOffsets.count() == 0)
  }
}
