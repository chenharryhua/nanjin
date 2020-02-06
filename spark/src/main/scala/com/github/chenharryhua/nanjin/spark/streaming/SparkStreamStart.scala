package com.github.chenharryhua.nanjin.spark.streaming

import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJFailOnDataLoss, NJPath}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.OutputMode
import shapeless.ops.hlist.Selector
import shapeless.{::, HList}

final class SparkStreamStart[F[_], HL <: HList, A: TypedEncoder](
  ds: Dataset[A],
  params: StreamParams[HL])
    extends Serializable {
  @transient lazy val typedDataset: TypedDataset[A] = TypedDataset.create(ds)

  def filter(f: A => Boolean): SparkStreamStart[F, HL, A] =
    new SparkStreamStart[F, HL, A](ds.filter(f), params)

  def map[B: TypedEncoder](f: A => B): SparkStreamStart[F, HL, B] =
    new SparkStreamStart[F, HL, B](typedDataset.deserialized.map(f).dataset, params)

  def flatMap[B: TypedEncoder](f: A => TraversableOnce[B]): SparkStreamStart[F, HL, B] =
    new SparkStreamStart[F, HL, B](typedDataset.deserialized.flatMap(f).dataset, params)

  def withCheckpoint(cp: String): SparkStreamStart[F, NJCheckpoint :: HL, A] =
    new SparkStreamStart[F, NJCheckpoint :: HL, A](ds, params.withCheckpoint(cp))

  def withPath(path: String): SparkStreamStart[F, NJPath :: HL, A] =
    new SparkStreamStart(ds, params.withPath(path))

  def withJsonFormat: SparkStreamStart[F, NJFileFormat :: HL, A] =
    new SparkStreamStart(ds, params.withFileFormat(NJFileFormat.Json))

  def withAvroFormat: SparkStreamStart[F, NJFileFormat :: HL, A] =
    new SparkStreamStart(ds, params.withFileFormat(NJFileFormat.Avro))

  def withParquetFormat: SparkStreamStart[F, NJFileFormat :: HL, A] =
    new SparkStreamStart(ds, params.withFileFormat(NJFileFormat.Parquet))

  def withAppendMode: SparkStreamStart[F, OutputMode :: HL, A] =
    new SparkStreamStart(ds, params.withMode(OutputMode.Append))

  def withUpdateMode: SparkStreamStart[F, OutputMode :: HL, A] =
    new SparkStreamStart(ds, params.withMode(OutputMode.Update))

  def withCompleteMode: SparkStreamStart[F, OutputMode :: HL, A] =
    new SparkStreamStart(ds, params.withMode(OutputMode.Complete))

  def withoutFailOnDtaLoss: SparkStreamStart[F, NJFailOnDataLoss :: HL, A] =
    new SparkStreamStart(ds, params.withoutFailOnDataLoss)

  // sinks

  def consoleSink(numRows: Int = 20, trucate: Boolean = false)(
    implicit
    dataLoss: Selector[HL, NJFailOnDataLoss]
  ): SparkStreamRunner[F, A] =
    new SparkStreamRunner(ds.writeStream, ConsoleSink(numRows, trucate, dataLoss(params.hl)))

  def fileSink(
    implicit
    path: Selector[HL, NJPath],
    checkpoint: Selector[HL, NJCheckpoint],
    fileFormat: Selector[HL, NJFileFormat],
    dataLoss: Selector[HL, NJFailOnDataLoss]) =
    new SparkStreamRunner(
      ds.writeStream,
      FileSink(fileFormat(params.hl), path(params.hl), checkpoint(params.hl), dataLoss(params.hl)))

  def kafkaSink[K, V](kit: KafkaTopicKit[K, V])(
    implicit pr: A =:= NJProducerRecord[K, V],
    mode: Selector[HL, OutputMode],
    checkpoint: Selector[HL, NJCheckpoint],
    dataLoss: Selector[HL, NJFailOnDataLoss])
    : SparkStreamRunner[F, NJProducerRecord[Array[Byte], Array[Byte]]] =
    new SparkStreamRunner[F, NJProducerRecord[Array[Byte], Array[Byte]]](
      typedDataset.deserialized
        .map(m =>
          pr(m).bimap(
            k => kit.codec.keySerde.serializer.serialize(kit.topicName.value, k),
            v => kit.codec.valueSerde.serializer.serialize(kit.topicName.value, v)))
        .dataset
        .writeStream,
      KafkaSink(
        mode(params.hl),
        kit.settings.brokers.get,
        kit.topicName,
        checkpoint(params.hl),
        dataLoss(params.hl)))
}
