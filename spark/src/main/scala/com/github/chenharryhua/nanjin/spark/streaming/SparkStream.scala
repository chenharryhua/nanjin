package com.github.chenharryhua.nanjin.spark.streaming

import cats.implicits._
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.KafkaTopicKit
import com.github.chenharryhua.nanjin.kafka.common.NJProducerRecord
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

trait SparkStreamUpdateParams[A] extends UpdateParams[StreamConfig, A] with Serializable {
  def params: StreamParams
}

final class SparkStream[F[_], A: TypedEncoder](ds: Dataset[A], cfg: StreamConfig)
    extends SparkStreamUpdateParams[SparkStream[F, A]] {

  override val params: StreamParams = StreamConfigF.evalConfig(cfg)

  override def withParamUpdate(f: StreamConfig => StreamConfig): SparkStream[F, A] =
    new SparkStream[F, A](ds, f(cfg))

  @transient lazy val typedDataset: TypedDataset[A] = TypedDataset.create(ds)

  // transforms

  def filter(f: A => Boolean): SparkStream[F, A] =
    new SparkStream[F, A](ds.filter(f), cfg)

  def map[B: TypedEncoder](f: A => B): SparkStream[F, B] =
    new SparkStream[F, B](typedDataset.deserialized.map(f).dataset, cfg)

  def flatMap[B: TypedEncoder](f: A => TraversableOnce[B]): SparkStream[F, B] =
    new SparkStream[F, B](typedDataset.deserialized.flatMap(f).dataset, cfg)

  def transform[B: TypedEncoder](f: TypedDataset[A] => TypedDataset[B]) =
    new SparkStream[F, B](f(typedDataset).dataset, cfg)

  // sinks

  def consoleSink: NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](ds.writeStream, cfg)

  def fileSink(path: String): NJFileSink[F, A] =
    new NJFileSink[F, A](ds.writeStream, cfg, path)

  def kafkaSink[K: TypedEncoder, V: TypedEncoder](kit: KafkaTopicKit[K, V])(
    implicit ev: A =:= NJProducerRecord[K, V]): NJKafkaSink[F] =
    new KafkaPRStream[F, K, V](typedDataset.deserialized.map(ev).dataset, cfg).kafkaSink(kit)
}
