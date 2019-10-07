package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDate

import com.github.chenharryhua.nanjin.codec.SparkafkaConsumerRecord
import com.github.chenharryhua.nanjin.kafka.KafkaTimestamp
import com.github.chenharryhua.nanjin.sparkdb.DatetimeInjectionInstances._
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}

final case class MinutelyAggResult(minute: Int, count: Long)
final case class HourlyAggResult(hour: Int, count: Long)
final case class DailyAggResult(date: LocalDate, count: Long)

trait Aggregations {

  implicit class PredefinedAggregationFunction[K: TypedEncoder, V: TypedEncoder](
    tds: TypedDataset[SparkafkaConsumerRecord[K, V]]) {

    def minutely: TypedDataset[MinutelyAggResult] = {
      val minute: TypedDataset[Int] = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local.getMinute
      }
      val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[MinutelyAggResult]
      res.orderBy(res('minute).asc)
    }

    def hourly: TypedDataset[HourlyAggResult] = {
      val hour = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local.getHour
      }
      val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[HourlyAggResult]
      res.orderBy(res('hour).asc)
    }

    def daily: TypedDataset[DailyAggResult] = {
      val day: TypedDataset[LocalDate] = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local.toLocalDate
      }
      val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
      res.orderBy(res('date).asc)
    }
  }
}
