package com.github.chenharryhua.nanjin.sparkafka

import cats.effect.{ConcurrentEffect, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import frameless.TypedDataset
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.producer.RecordMetadata

private[sparkafka] trait DatasetExtensions {

  implicit final class SparKafkaUploadSyntax[K, V](
    data: TypedDataset[SparKafkaProducerRecord[K, V]]) {

    def kafkaUpload[F[_]: ConcurrentEffect: Timer](
      topic: => KafkaTopic[F, K, V],
      rate: KafkaUploadRate = KafkaUploadRate.default): Stream[F, Chunk[RecordMetadata]] =
      SparKafka.uploadToKafka(topic, data, rate)
  }
}
