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
  implicit final class SparkDBSyntax[A](data: TypedDataset[A]) {
    def dbUpload[F[_]](db: TableDataset[F, A]): F[Unit] = db.uploadToDB(data)
  }

  implicit final class SparkafkaUploadSyntax[K, V](
    data: TypedDataset[SparKafkaProducerRecord[K, V]])(implicit sk: SparKafkaSession) {

    def kafkaUpload[F[_]: ConcurrentEffect: Timer](topic: => KafkaTopic[F, K, V]): F[Unit] =
      SparKafka.uploadToKafka[F, K, V](topic, data, sk.params.uploadRate).compile.drain
  }
}
