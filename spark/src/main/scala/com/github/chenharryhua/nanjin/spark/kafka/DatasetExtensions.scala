package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, Timer}
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.UploadRate
import frameless.{TypedDataset, TypedEncoder}
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.producer.RecordMetadata

private[kafka] trait DatasetExtensions {

  implicit final class SparKafkaUploadSyntax[K, V](
    data: TypedDataset[SparKafkaProducerRecord[K, V]]) {

    def kafkaUpload[F[_]: ConcurrentEffect: Timer](
      topic: => KafkaTopic[F, K, V],
      rate: UploadRate = UploadRate.default): Stream[F, Chunk[RecordMetadata]] =
      SparKafka.uploadToKafka(topic, data, rate)
  }

  implicit final class SparKafkaConsumerRecordSyntax[K: TypedEncoder, V: TypedEncoder](
    consumerRecords: TypedDataset[SparKafkaConsumerRecord[K, V]]
  ) {

    def nullValues: TypedDataset[SparKafkaConsumerRecord[K, V]] =
      consumerRecords.filter(consumerRecords('value).isNone)

    def nullKeys: TypedDataset[SparKafkaConsumerRecord[K, V]] =
      consumerRecords.filter(consumerRecords('key).isNone)

    def values: TypedDataset[V] =
      consumerRecords.select(consumerRecords('value)).as[Option[V]].deserialized.flatMap(x => x)

    def keys: TypedDataset[K] =
      consumerRecords.select(consumerRecords('key)).as[Option[K]].deserialized.flatMap(x => x)
  }

}
