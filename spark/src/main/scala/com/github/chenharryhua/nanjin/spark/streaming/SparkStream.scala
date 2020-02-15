package com.github.chenharryhua.nanjin.spark.streaming

import cats.implicits._
import com.github.chenharryhua.nanjin.common.UpdateParams
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

  // sinks

  def consoleSink: NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](ds.writeStream, cfg)

  def fileSink(path: String): NJFileSink[F, A] =
    new NJFileSink[F, A](ds.writeStream, cfg, path)
}
