package com.github.chenharryhua.nanjin.sparkafka

import cats.effect.{ConcurrentEffect, Timer}
import com.github.chenharryhua.nanjin.sparkdb.TableDataset
import frameless.TypedDataset
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import fs2.Chunk
import org.apache.kafka.clients.producer.RecordMetadata
import fs2.Stream

private[sparkafka] trait SparkDatasetExtensions {

  implicit final class SparkDBSyntax[A](data: TypedDataset[A]) {
    def dbUpload[F[_]](db: TableDataset[F, A]): F[Unit] = db.uploadToDB(data)
  }

  implicit final class SparKafkaUploadSyntax[K, V](
    data: TypedDataset[SparKafkaProducerRecord[K, V]]) {

    def kafkaUpload[F[_]: ConcurrentEffect: Timer](
      topic: => KafkaTopic[F, K, V],
      rate: KafkaUploadRate = KafkaUploadRate.default): Stream[F, Chunk[RecordMetadata]] =
      SparKafka.uploadToKafka(topic, data, rate)
  }
}
