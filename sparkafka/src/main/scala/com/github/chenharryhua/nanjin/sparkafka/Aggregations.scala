package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDate

import com.github.chenharryhua.nanjin.codec.SparkafkaConsumerRecord
import com.github.chenharryhua.nanjin.kafka.KafkaTimestamp
import com.github.chenharryhua.nanjin.sparkdb.DatetimeInjectionInstances._
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}

final case class AggResult(key: Int, value: Long)
final case class DailyAggResult(key: LocalDate, value: Long)

trait Aggregations {

  implicit class PredefinedAggregationFunction[K: TypedEncoder, V: TypedEncoder](
    tds: TypedDataset[SparkafkaConsumerRecord[K, V]]) {

    def hourly: TypedDataset[AggResult] = {
      val hour = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local.getHour
      }
      val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[AggResult]
      res.orderBy(res('key).asc)
    }

    def minutely: TypedDataset[AggResult] = {
      val minute: TypedDataset[Int] = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local.getMinute
      }
      val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[AggResult]
      res.orderBy(res('key).asc)
    }

    def daily: TypedDataset[DailyAggResult] = {
      val day: TypedDataset[LocalDate] = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local.toLocalDate
      }
      val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
      res.orderBy(res('key).asc)
    }
  }
}
