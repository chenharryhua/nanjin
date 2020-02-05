package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJPath}

final class SparkStreamTransformer[F[_], A: TypedEncoder](ds: Dataset[A], checkpoint: NJCheckpoint)
    extends Serializable {
  @transient lazy val typedDataset: TypedDataset[A] = TypedDataset.create(ds)

  def filter(f: A => Boolean): SparkStreamTransformer[F, A] =
    new SparkStreamTransformer[F, A](ds.filter(f), checkpoint)

  def map[B: TypedEncoder](f: A => B): SparkStreamTransformer[F, B] =
    new SparkStreamTransformer[F, B](typedDataset.deserialized.map(f).dataset, checkpoint)

  def withCheckpoint(cp: String): SparkStreamTransformer[F, A] =
    new SparkStreamTransformer[F, A](ds, NJCheckpoint(cp))

  def flatMap[B: TypedEncoder](f: A => TraversableOnce[B]): SparkStreamTransformer[F, B] =
    new SparkStreamTransformer[F, B](typedDataset.deserialized.flatMap(f).dataset, checkpoint)

  def withFileSink(fileFormat: NJFileFormat, path: String): SparkStreamRunner[F, A] =
    new SparkStreamRunner[F, A](
      ds.writeStream,
      FileSink.append(fileFormat, NJPath(path), checkpoint))

  def withKafkaSink[K, V](kit: KafkaTopicKit[K, V])(implicit ev: A =:= NJProducerRecord[K, V])
    : SparkStreamRunner[F, NJProducerRecord[Array[Byte], Array[Byte]]] =
    new SparkStreamRunner[F, NJProducerRecord[Array[Byte], Array[Byte]]](
      typedDataset.deserialized
        .map(m =>
          ev(m).bimap(
            k => kit.codec.keySerde.serializer.serialize(kit.topicName.value, k),
            v => kit.codec.valueSerde.serializer.serialize(kit.topicName.value, v)))
        .dataset.writeStream,
      KafkaSink.update(kit.settings.brokers.get, kit.topicName, checkpoint))
}
