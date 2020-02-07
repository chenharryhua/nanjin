package com.github.chenharryhua.nanjin.spark.streaming

import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import com.github.chenharryhua.nanjin.spark.NJPath
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class SparkStreamStart[F[_], A: TypedEncoder](ds: Dataset[A], params: StreamParams)
    extends Serializable {
  @transient lazy val typedDataset: TypedDataset[A] = TypedDataset.create(ds)

  // transforms

  def filter(f: A => Boolean): SparkStreamStart[F, A] =
    new SparkStreamStart[F, A](ds.filter(f), params)

  def map[B: TypedEncoder](f: A => B): SparkStreamStart[F, B] =
    new SparkStreamStart[F, B](typedDataset.deserialized.map(f).dataset, params)

  def flatMap[B: TypedEncoder](f: A => TraversableOnce[B]): SparkStreamStart[F, B] =
    new SparkStreamStart[F, B](typedDataset.deserialized.flatMap(f).dataset, params)

  // sinks

  def consoleSink: NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](ds.writeStream, params.showDs, params.dataLoss)

  def fileSink(path: String, checkpoint: String): NJFileSink[F, A] =
    new NJFileSink[F, A](
      ds.writeStream,
      params.fileFormat,
      NJPath(path),
      NJCheckpoint(checkpoint),
      params.dataLoss)

  def kafkaSink[K, V](kit: KafkaTopicKit[K, V], checkpoint: String)(
    implicit pr: A =:= NJProducerRecord[K, V]): NJKafkaSink[F] =
    new NJKafkaSink[F](
      typedDataset.deserialized
        .map(m =>
          pr(m).bimap(
            k => kit.codec.keySerde.serializer.serialize(kit.topicName.value, k),
            v => kit.codec.valueSerde.serializer.serialize(kit.topicName.value, v)))
        .dataset
        .writeStream,
      params.outputMode,
      kit.settings.brokers.get,
      kit.topicName,
      NJCheckpoint(checkpoint),
      params.dataLoss)
}
