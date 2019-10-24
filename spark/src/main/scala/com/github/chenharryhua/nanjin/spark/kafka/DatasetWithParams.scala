package com.github.chenharryhua.nanjin.spark.kafka

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}

import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark._
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final case class MinutelyAggResult(minute: Int, count: Long)
final case class HourlyAggResult(hour: Int, count: Long)
final case class DailyAggResult(date: LocalDate, count: Long)
final case class DailyHourAggResult(date: LocalDateTime, count: Long)
final case class DailyMinuteAggResult(date: LocalDateTime, count: Long)

final case class ConsumerRecordDatasetWithParams[K: TypedEncoder, V: TypedEncoder](
  params: SparKafkaParams,
  private val crs: Dataset[SparKafkaConsumerRecord[K, V]]) {

  def consumerRecords: TypedDataset[SparKafkaConsumerRecord[K, V]] = TypedDataset.create(crs)

  def minutely: TypedDataset[MinutelyAggResult] = {
    val minute: TypedDataset[Int] = consumerRecords.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(params.zoneId).getMinute
    }
    val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[MinutelyAggResult]
    res.orderBy(res('minute).asc)
  }

  def hourly: TypedDataset[HourlyAggResult] = {
    val hour = consumerRecords.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(params.zoneId).getHour
    }
    val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[HourlyAggResult]
    res.orderBy(res('hour).asc)
  }

  def daily: TypedDataset[DailyAggResult] = {
    val day: TypedDataset[LocalDate] = consumerRecords.deserialized.map { m =>
      NJTimestamp(m.timestamp).atZone(params.zoneId).toLocalDate
    }
    val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
    res.orderBy(res('date).asc)
  }

  def dailyHour: TypedDataset[DailyHourAggResult] = {
    implicit val zoneId: ZoneId = params.zoneId
    val dayHour: TypedDataset[LocalDateTime] = consumerRecords.deserialized.map { m =>
      val dt   = NJTimestamp(m.timestamp).atZone(params.zoneId).toLocalDateTime
      val hour = dt.getHour
      LocalDateTime.of(dt.toLocalDate, LocalTime.of(hour, 0))
    }
    val res = dayHour.groupBy(dayHour.asCol).agg(count(dayHour.asCol)).as[DailyHourAggResult]
    res.orderBy(res('date).asc)
  }

  def dailyMinute: TypedDataset[DailyMinuteAggResult] = {
    implicit val zoneId: ZoneId = params.zoneId
    val dayMinute: TypedDataset[LocalDateTime] = consumerRecords.deserialized.map { m =>
      val dt   = NJTimestamp(m.timestamp).atZone(params.zoneId).toLocalDateTime
      val hour = dt.getHour
      val min  = dt.getMinute
      LocalDateTime.of(dt.toLocalDate, LocalTime.of(hour, min))
    }
    val res =
      dayMinute.groupBy(dayMinute.asCol).agg(count(dayMinute.asCol)).as[DailyMinuteAggResult]
    res.orderBy(res('date).asc)
  }

  def toProducerRecords: TypedDataset[SparKafkaProducerRecord[K, V]] =
    SparKafka.toProducerRecords(consumerRecords, params.conversionStrategy, params.clock)
}
