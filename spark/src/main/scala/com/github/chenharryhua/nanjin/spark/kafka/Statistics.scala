package com.github.chenharryhua.nanjin.spark.kafka

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}

import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.datetime.iso._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset
import com.github.chenharryhua.nanjin.kafka.{NJConsumerRecord, NJProducerRecord}

final case class MinutelyAggResult(minute: Int, count: Long)
final case class HourlyAggResult(hour: Int, count: Long)
final case class DailyAggResult(date: LocalDate, count: Long)
final case class DailyHourAggResult(date: LocalDateTime, count: Long)
final case class DailyMinuteAggResult(date: LocalDateTime, count: Long)

final class Statistics[K: TypedEncoder, V: TypedEncoder](
  zoneId: ZoneId,
  private val crs: Dataset[NJConsumerRecord[K, V]])
    extends Serializable {

  private def consumerRecords: TypedDataset[NJConsumerRecord[K, V]] = TypedDataset.create(crs)

  def minutely: TypedDataset[MinutelyAggResult] = {
    val minute: TypedDataset[Int] = consumerRecords.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(zoneId).getMinute
    }
    val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[MinutelyAggResult]
    res.orderBy(res('minute).asc)
  }

  def hourly: TypedDataset[HourlyAggResult] = {
    val hour = consumerRecords.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(zoneId).getHour
    }
    val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[HourlyAggResult]
    res.orderBy(res('hour).asc)
  }

  def daily: TypedDataset[DailyAggResult] = {
    val day: TypedDataset[LocalDate] = consumerRecords.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(zoneId).toLocalDate
    }
    val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
    res.orderBy(res('date).asc)
  }

  def dailyHour: TypedDataset[DailyHourAggResult] = {
    val dayHour: TypedDataset[LocalDateTime] = consumerRecords.deserialized.map { m =>
      val dt = NJTimestamp(m.timestamp).atZone(zoneId).toLocalDateTime
      LocalDateTime.of(dt.toLocalDate, LocalTime.of(dt.getHour, 0))
    }
    val res = dayHour.groupBy(dayHour.asCol).agg(count(dayHour.asCol)).as[DailyHourAggResult]
    res.orderBy(res('date).asc)
  }

  def dailyMinute: TypedDataset[DailyMinuteAggResult] = {
    val dayMinute: TypedDataset[LocalDateTime] = consumerRecords.deserialized.map { m =>
      val dt = NJTimestamp(m.timestamp).atZone(zoneId).toLocalDateTime
      LocalDateTime.of(dt.toLocalDate, LocalTime.of(dt.getHour, dt.getMinute))
    }
    val res =
      dayMinute.groupBy(dayMinute.asCol).agg(count(dayMinute.asCol)).as[DailyMinuteAggResult]
    res.orderBy(res('date).asc)
  }
}
