package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, utcTime, NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDate, LocalDateTime}
import scala.concurrent.duration._

class SKConfigTest extends AnyFunSuite {
  val skc = SKConfig(TopicName("config.test"), sydneyTime).zone_id(utcTime)
  test("date-time parameters") {
    val d1 = NJTimestamp("10:00", sydneyTime)
    val d2 = NJTimestamp("11:00", sydneyTime)

    val p1 = skc.time_range("10:00", "11:00").zone_id(sydneyTime)
    assert(p1.evalConfig.timeRange.startTimestamp.contains(d1))
    assert(p1.evalConfig.timeRange.endTimestamp.contains(d2))
    assert(p1.evalConfig.timeRange.zoneId == sydneyTime)

    val p2 = skc
      .end_time(LocalDateTime.now())
      .start_time(LocalDateTime.now)
      .start_time("10:00")
      .end_time("11:00")
      .zone_id(sydneyTime)
    assert(p2.evalConfig.timeRange.startTimestamp.contains(d1))
    assert(p2.evalConfig.timeRange.endTimestamp.contains(d2))
    assert(p2.evalConfig.timeRange.zoneId == sydneyTime)

    val dr = NJDateTimeRange(sydneyTime).withStartTime("10:00").withEndTime("11:00")

    val p3 = skc
      .time_range_n_seconds(1)
      .time_range_today
      .time_range_yesterday
      .time_range_one_day(LocalDate.of(2012, 10, 26))
      .time_range_one_day("2012-10-26")
      .time_range(dr)
    assert(p3.evalConfig.timeRange.startTimestamp.contains(d1))
    assert(p3.evalConfig.timeRange.endTimestamp.contains(d2))
    assert(p3.evalConfig.timeRange.zoneId == sydneyTime)
  }

  test("upload parameters") {
    val p = skc
      .load_bulk_size(1)
      .load_interval(0.1.second)
      .load_records_limit(10)
      .load_time_limit(1.minutes)
      .evalConfig
      .loadParams

    assert(p.bulkSize == 1)
    assert(p.interval == 100.millisecond)
    assert(p.recordsLimit == 10)
    assert(p.timeLimit == 60.second)
  }
  test("misc update") {
    val p = skc
      .topic_name("config.update")
      .location_strategy(LocationStrategies.PreferBrokers)
      .replay_path_builder(_.value)
      .evalConfig

    assert(p.topicName.value == "config.update")
    assert(p.replayPath == "config.update")
  }
}
