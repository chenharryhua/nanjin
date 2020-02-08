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
    NJConsoleSink[F, A](ds.writeStream, params.showDs, params.dataLoss, params.trigger)

  def fileSink(path: String): NJFileSink[F, A] =
    NJFileSink[F, A](
      ds.writeStream,
      params.fileFormat,
      NJPath(path),
      params.checkpoint.append("fileSink"),
      params.dataLoss,
      params.trigger)

  def kafkaSink[K, V](kit: KafkaTopicKit[K, V])(
    implicit pr: A =:= NJProducerRecord[K, V]): NJKafkaSink[F] =
    NJKafkaSink[F](
      typedDataset.deserialized
        .map(m =>
          pr(m).bimap(
            k => kit.codec.keySerde.serializer.serialize(kit.topicName.value, k),
            v => kit.codec.valSerde.serializer.serialize(kit.topicName.value, v)))
        .dataset
        .writeStream,
      params.outputMode,
      kit.settings.producerSettings,
      kit.topicName,
      params.checkpoint.append(s"uploadTo/${kit.topicName.value}"),
      params.dataLoss,
      params.trigger
    )
}
