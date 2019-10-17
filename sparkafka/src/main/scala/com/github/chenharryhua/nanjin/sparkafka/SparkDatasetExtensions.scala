package com.github.chenharryhua.nanjin.sparkafka

import java.time.{LocalDate, ZoneId}

import cats.effect.{ConcurrentEffect, Timer}
import com.github.chenharryhua.nanjin.kafka.{KafkaTimestamp, KafkaTopic}
import com.github.chenharryhua.nanjin.sparkdb.TableDataset
import frameless.{TypedDataset, TypedEncoder}
import frameless.functions.aggregate.count

final case class MinutelyAggResult(minute: Int, count: Long)
final case class HourlyAggResult(hour: Int, count: Long)
final case class DailyAggResult(date: LocalDate, count: Long)

private[sparkafka] trait SparkDatasetExtensions {

  implicit final class SparkafkaConsumerRecordSyntax[K: TypedEncoder, V: TypedEncoder](
    tds: TypedDataset[SparKafkaConsumerRecord[K, V]]) {

    def minutely(zoneId: ZoneId = ZoneId.systemDefault): TypedDataset[MinutelyAggResult] = {
      val minute: TypedDataset[Int] = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local(zoneId).getMinute
      }
      val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[MinutelyAggResult]
      res.orderBy(res('minute).asc)
    }

    def hourly(zoneId: ZoneId = ZoneId.systemDefault): TypedDataset[HourlyAggResult] = {
      val hour = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local(zoneId).getHour
      }
      val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[HourlyAggResult]
      res.orderBy(res('hour).asc)
    }

    def daily(zoneId: ZoneId = ZoneId.systemDefault): TypedDataset[DailyAggResult] = {
      val day: TypedDataset[LocalDate] = tds.deserialized.map { m =>
        KafkaTimestamp(m.timestamp).local(zoneId).toLocalDate
      }
      val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
      res.orderBy(res('date).asc)
    }

    def nullValues: TypedDataset[SparKafkaConsumerRecord[K, V]] =
      tds.filter(tds('value).isNone)

    def nullKeys: TypedDataset[SparKafkaConsumerRecord[K, V]] =
      tds.filter(tds('key).isNone)

    def values: TypedDataset[V] =
      tds.select(tds('value)).as[Option[V]].deserialized.flatMap(x => x)

    def keys: TypedDataset[K] =
      tds.select(tds('key)).as[Option[K]].deserialized.flatMap(x => x)

    def toProducerRecords(cs: ConversionStrategy = ConversionStrategy.Intact)
      : TypedDataset[SparKafkaProducerRecord[K, V]] =
      SparKafka.toProducerRecords(tds, cs)
  }

  implicit final class SparkDBSyntax[A](data: TypedDataset[A]) {
    def dbUpload[F[_]](db: TableDataset[F, A]): F[Unit] = db.uploadToDB(data)
  }

  implicit final class SparkafkaUploadSyntax[K, V](
    data: TypedDataset[SparKafkaProducerRecord[K, V]])(implicit sk: SparKafkaSession) {

    def kafkaUpload[F[_]: ConcurrentEffect: Timer](topic: => KafkaTopic[F, K, V]): F[Unit] =
      SparKafka.uploadToKafka[F, K, V](topic, data, sk.params.uploadRate).compile.drain
  }
}
