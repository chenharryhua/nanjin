package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDate

import com.github.chenharryhua.nanjin.kafka.{KafkaTimestamp, KafkaTopic}
import DatetimeInjectionInstances._
import cats.effect.{Concurrent, Timer}
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import cats.effect.ConcurrentEffect

final case class MinutelyAggResult(minute: Int, count: Long)
final case class HourlyAggResult(hour: Int, count: Long)
final case class DailyAggResult(date: LocalDate, count: Long)

trait KafkaDatasetSyntax {

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

    def nullValues: TypedDataset[SparkafkaConsumerRecord[K, V]] =
      tds.filter(tds('value).isNone)

    def nullKeys: TypedDataset[SparkafkaConsumerRecord[K, V]] =
      tds.filter(tds('key).isNone)

    def values: TypedDataset[V] =
      tds.select(tds('value)).as[Option[V]].deserialized.flatMap(x => x)

    def dbUpload[F[_]](db: TableDataset[F, V])(implicit spark: SparkSession): F[Unit] =
      db.uploadToDB(values)

    def toIntactProducerRecord: TypedDataset[SparkafkaProducerRecord[K, V]] =
      tds.orderBy(tds('timestamp).asc, tds('offset).asc).deserialized.map { scr =>
        scr.toSparkafkaProducerRecord
      }

    def toCleanedProducerRecord: TypedDataset[SparkafkaProducerRecord[K, V]] =
      toIntactProducerRecord.deserialized.map(_.withoutPartition.withoutTimestamp)

    def kafkaUpload[F[_]: ConcurrentEffect: Timer](topic: => KafkaTopic[F, K, V]): F[Unit] = {
      val sorted = tds.orderBy(tds('timestamp).asc, tds('offset).asc).deserialized.map { scr =>
        scr.toSparkafkaProducerRecord
      }
      Sparkafka.uploadToKafka[F, K, V](toIntactProducerRecord, topic, 5000).compile.drain
    }
  }
}
