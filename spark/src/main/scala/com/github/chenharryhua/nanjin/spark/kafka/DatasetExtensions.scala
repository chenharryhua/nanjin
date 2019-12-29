package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, Timer}
import com.github.chenharryhua.nanjin.kafka.codec.NJProducerRecord
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.common.NJRate
import com.github.chenharryhua.nanjin.kafka.codec.{NJConsumerRecord, NJProducerRecord}
import frameless.{TypedDataset, TypedEncoder}
import fs2.{Chunk, Stream}
import org.apache.kafka.clients.producer.RecordMetadata

private[kafka] trait DatasetExtensions {

  implicit final class SparKafkaUploadSyntax[K, V](val data: TypedDataset[NJProducerRecord[K, V]]) {

    def kafkaUpload[F[_]: ConcurrentEffect: Timer](
      topic: => KafkaTopic[F, K, V],
      rate: NJRate = NJRate.default): Stream[F, Chunk[RecordMetadata]] =
      SparKafka.uploadToKafka(topic, data, rate)
  }

  implicit final class SparKafkaConsumerRecordSyntax[K: TypedEncoder, V: TypedEncoder](
    val consumerRecords: TypedDataset[NJConsumerRecord[K, V]]
  ) {

    def nullValues: TypedDataset[NJConsumerRecord[K, V]] =
      consumerRecords.filter(consumerRecords('value).isNone)

    def nullKeys: TypedDataset[NJConsumerRecord[K, V]] =
      consumerRecords.filter(consumerRecords('key).isNone)

    def values: TypedDataset[V] =
      consumerRecords.select(consumerRecords('value)).as[Option[V]].deserialized.flatMap(x => x)

    def keys: TypedDataset[K] =
      consumerRecords.select(consumerRecords('key)).as[Option[K]].deserialized.flatMap(x => x)
  }
}
