package mtest.spark.kafka

import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.concurrent.TimeUnit

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaParams
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.FiniteDuration

class SparKafkaParamTest extends AnyFunSuite {
  test("order of applying time data does not matter") {
    val zoneId    = ZoneId.of("Asia/Chongqing")
    val startTime = LocalDateTime.of(2012, 10, 26, 18, 0, 0)
    val endTime   = LocalDateTime.of(2012, 10, 26, 23, 0, 0)

    val param = SparKafkaParams.default

    val a = param.withEndTime(endTime).withZoneId(zoneId).withStartTime(startTime)
    val b = param.withStartTime(startTime).withZoneId(zoneId).withEndTime(endTime)

    assert(a.timeRange === b.timeRange)
  }
  test("within one day") {
    val zoneId = ZoneId.of("Australia/Sydney")
    val date   = LocalDate.of(2012, 10, 26)
    val param  = SparKafkaParams.default
    val k      = param.withinOneDay(date).withZoneId(zoneId)
    assert(k.timeRange.duration.get === FiniteDuration(86400000L, TimeUnit.MILLISECONDS))
    assert(k.timeRange.start.get === NJTimestamp(1351170000000L))
    assert(k.timeRange.end.get === NJTimestamp(1351256400000L))
  }
}
