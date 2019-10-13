package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDate

import cats.effect.{ConcurrentEffect, Timer}
import com.github.chenharryhua.nanjin.kafka.{ConversionStrategy, KafkaTimestamp, KafkaTopic}
import com.github.chenharryhua.nanjin.sparkafka.DatetimeInjectionInstances._
import com.github.chenharryhua.nanjin.sparkdb.TableDataset
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import cats.effect.Sync

final case class MinutelyAggResult(minute: Int, count: Long)
final case class HourlyAggResult(hour: Int, count: Long)
final case class DailyAggResult(date: LocalDate, count: Long)

trait KafkaDatasetSyntax {

  implicit final class SparkafkaDbSyntax[K: TypedEncoder, V: TypedEncoder](
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

    def producerRecords[F[_]](
      topic: => KafkaTopic[F, K, V]): TypedDataset[SparkafkaProducerRecord[K, V]] = {
      val sorted = tds.orderBy(tds('timestamp).asc, tds('offset).asc)
      topic.sparkafkaParams.conversionStrategy match {
        case ConversionStrategy.Intact =>
          sorted.deserialized.map(_.toSparkafkaProducerRecord)
        case ConversionStrategy.RemovePartition =>
          sorted.deserialized.map(_.toSparkafkaProducerRecord.withoutPartition)
        case ConversionStrategy.RemoveTimestamp =>
          sorted.deserialized.map(_.toSparkafkaProducerRecord.withoutTimestamp)
        case ConversionStrategy.RemovePartitionAndTimestamp =>
          sorted.deserialized.map(_.toSparkafkaProducerRecord.withoutTimestamp.withoutPartition)
      }
    }

    def kafkaUpload[F[_]: ConcurrentEffect: Timer](topic: => KafkaTopic[F, K, V]): F[Unit] =
      Sparkafka.uploadToKafka[F, K, V](producerRecords(topic), topic).compile.drain
  }

  implicit final class SparkafkaTopicSyntax[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V])(implicit spark: SparkSession) {

    def datasetFromKafka: F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
      Sparkafka.datasetFromKafka(topic)

    def datasetFromDisk: F[TypedDataset[SparkafkaConsumerRecord[K, V]]] =
      Sparkafka.datasetFromDisk(topic)
  }
}
