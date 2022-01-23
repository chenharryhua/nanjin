package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, utcTime, NJDateTimeRange, NJTimestamp}
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.scalatest.funsuite.AnyFunSuite
import squants.information.Kilobytes

import java.time.{LocalDate, LocalDateTime}
import scala.concurrent.duration.*

class SKConfigTest extends AnyFunSuite {
  val skc: SKConfig = SKConfig(TopicName("config.test"), sydneyTime).zoneId(utcTime)
  test("date-time parameters") {
    val d1 = NJTimestamp("10:00", sydneyTime)
    val d2 = NJTimestamp("11:00", sydneyTime)

    val p1 = skc.timeRange("10:00", "11:00").zoneId(sydneyTime)
    assert(p1.evalConfig.timeRange.startTimestamp.contains(d1))
    assert(p1.evalConfig.timeRange.endTimestamp.contains(d2))
    assert(p1.evalConfig.timeRange.zoneId == sydneyTime)

    val p2 = skc
      .endTime(LocalDateTime.now())
      .startTime(LocalDateTime.now)
      .startTime("10:00")
      .endTime("11:00")
      .zoneId(sydneyTime)
    assert(p2.evalConfig.timeRange.startTimestamp.contains(d1))
    assert(p2.evalConfig.timeRange.endTimestamp.contains(d2))
    assert(p2.evalConfig.timeRange.zoneId == sydneyTime)

    val dr = NJDateTimeRange(sydneyTime).withStartTime("10:00").withEndTime("11:00")

    val p3 = skc
      .timeRangeNSeconds(1)
      .timeRangeToday
      .timeRangeYesterday
      .timeRangeOneDay(LocalDate.of(2012, 10, 26))
      .timeRangeOneDay("2012-10-26")
      .timeRange(dr)
    assert(p3.evalConfig.timeRange.startTimestamp.contains(d1))
    assert(p3.evalConfig.timeRange.endTimestamp.contains(d2))
    assert(p3.evalConfig.timeRange.zoneId == sydneyTime)
  }

  test("upload parameters") {
    val p =
      skc
        .loadThrottle(Kilobytes(1))
        .loadInterval(0.1.second)
        .loadRecordsLimit(10)
        .loadTimeLimit(1.minutes)
        .evalConfig
        .loadParams

    assert(p.throttle.toBytes.toInt == 1000)
    assert(p.interval == 100.millisecond)
    assert(p.recordsLimit == 10)
    assert(p.timeLimit == 60.second)
  }
  test("misc update") {
    val p = skc.topicName("config.update").locationStrategy(LocationStrategies.PreferBrokers).evalConfig

    assert(p.topicName.value == "config.update")
    assert(p.replayPath.pathStr == "data/sparKafka/config.update/replay")
  }
}
