package com.github.chenharryhua.nanjin.spark.streaming

import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import com.github.chenharryhua.nanjin.spark.NJPath
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

final class SparkStreamStart[F[_], A: TypedEncoder](
  ds: Dataset[A],
  params: StreamConfigParamF.ConfigParam)
    extends Serializable {
  @transient lazy val typedDataset: TypedDataset[A] = TypedDataset.create(ds)

  private val p: StreamParams = StreamConfigParamF.evalParams(params)

  def withJson: SparkStreamStart[F, A] =
    new SparkStreamStart[F, A](ds, StreamConfigParamF.withFileFormat(NJFileFormat.Json, params))

  def withAvro: SparkStreamStart[F, A] =
    new SparkStreamStart[F, A](ds, StreamConfigParamF.withFileFormat(NJFileFormat.Avro, params))

  def withParquet: SparkStreamStart[F, A] =
    new SparkStreamStart[F, A](ds, StreamConfigParamF.withFileFormat(NJFileFormat.Parquet, params))

  def withOutputMode(om: OutputMode): SparkStreamStart[F, A] =
    new SparkStreamStart[F, A](ds, StreamConfigParamF.withOutputMode(om, params))

  def withCheckpoint(cp: String): SparkStreamStart[F, A] =
    new SparkStreamStart[F, A](ds, StreamConfigParamF.withCheckpoint(cp, params))

  def withFailOnDataLoss(dl: Boolean): SparkStreamStart[F, A] =
    new SparkStreamStart[F, A](ds, StreamConfigParamF.withFailOnDataLoss(dl, params))

  def withTrigger(t: Trigger): SparkStreamStart[F, A] =
    new SparkStreamStart[F, A](ds, StreamConfigParamF.withTrigger(t, params))

  // transforms

  def filter(f: A => Boolean): SparkStreamStart[F, A] =
    new SparkStreamStart[F, A](ds.filter(f), params)

  def map[B: TypedEncoder](f: A => B): SparkStreamStart[F, B] =
    new SparkStreamStart[F, B](typedDataset.deserialized.map(f).dataset, params)

  def flatMap[B: TypedEncoder](f: A => TraversableOnce[B]): SparkStreamStart[F, B] =
    new SparkStreamStart[F, B](typedDataset.deserialized.flatMap(f).dataset, params)

  // sinks

  def consoleSink: NJConsoleSink[F, A] =
    NJConsoleSink[F, A](ds.writeStream, p.showDs, p.dataLoss, p.trigger)

  def fileSink(path: String): NJFileSink[F, A] =
    NJFileSink[F, A](
      ds.writeStream,
      p.fileFormat,
      NJPath(path),
      p.checkpoint.append("fileSink"),
      p.dataLoss,
      p.trigger)

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
      p.outputMode,
      kit.settings.producerSettings,
      kit.topicName,
      p.checkpoint.append(s"uploadTo/${kit.topicName.value}"),
      p.dataLoss,
      p.trigger
    )
}
