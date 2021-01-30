package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, utcTime, NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.TopicName
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDate, LocalDateTime}
import scala.concurrent.duration._

class SKConfigTest extends AnyFunSuite {
  val skc = SKConfig(TopicName("config.test"), sydneyTime).withZoneId(utcTime)
  test("date-time parameters") {
    val d1 = NJTimestamp("10:00", sydneyTime)
    val d2 = NJTimestamp("11:00", sydneyTime)

    val p1 = skc.withTimeRange("10:00", "11:00").withZoneId(sydneyTime)
    assert(p1.evalConfig.timeRange.startTimestamp.contains(d1))
    assert(p1.evalConfig.timeRange.endTimestamp.contains(d2))
    assert(p1.evalConfig.timeRange.zoneId == sydneyTime)

    val p2 = skc
      .withEndTime(LocalDateTime.now())
      .withStartTime(LocalDateTime.now)
      .withStartTime("10:00")
      .withEndTime("11:00")
      .withZoneId(sydneyTime)
    assert(p2.evalConfig.timeRange.startTimestamp.contains(d1))
    assert(p2.evalConfig.timeRange.endTimestamp.contains(d2))
    assert(p2.evalConfig.timeRange.zoneId == sydneyTime)

    val dr = NJDateTimeRange(sydneyTime).withStartTime("10:00").withEndTime("11:00")

    val p3 = skc
      .withNSeconds(1)
      .withToday
      .withYesterday
      .withOneDay(LocalDate.of(2012, 10, 26))
      .withOneDay("2012-10-26")
      .withTimeRange(dr)
    assert(p3.evalConfig.timeRange.startTimestamp.contains(d1))
    assert(p3.evalConfig.timeRange.endTimestamp.contains(d2))
    assert(p3.evalConfig.timeRange.zoneId == sydneyTime)
  }

  test("upload parameters") {
    val p = skc
      .withUploadBatchSize(1)
      .withUploadInterval(100.minute)
      .withUploadInterval(100)
      .withUploadRecordsLimit(10)
      .withUploadTimeLimit(1.minutes)
      .evalConfig
      .uploadParams

    assert(p.batchSize == 1)
    assert(p.interval == 100.millisecond)
    assert(p.recordsLimit == 10)
    assert(p.timeLimit == 60.second)
  }
  test("misc update") {
    val p = skc
      .withTopicName("config.update")
      .withLocationStrategy(LocationStrategies.PreferBrokers)
      .withReplayPathBuilder(_.value)
      .evalConfig

    assert(p.topicName.value == "config.update")
    assert(p.replayPath == "config.update")
  }
}
