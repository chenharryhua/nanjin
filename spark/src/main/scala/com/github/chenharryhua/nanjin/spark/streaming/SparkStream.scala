package com.github.chenharryhua.nanjin.spark.streaming

import cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

final class SparkStream[F[_], A: TypedEncoder](ds: Dataset[A], params: StreamConfigF.StreamConfig)
    extends Serializable {
  @transient lazy val typedDataset: TypedDataset[A] = TypedDataset.create(ds)

  private val p: StreamParams = StreamConfigF.evalParams(params)

  // transforms

  def filter(f: A => Boolean): SparkStream[F, A] =
    new SparkStream[F, A](ds.filter(f), params)

  def map[B: TypedEncoder](f: A => B): SparkStream[F, B] =
    new SparkStream[F, B](typedDataset.deserialized.map(f).dataset, params)

  def flatMap[B: TypedEncoder](f: A => TraversableOnce[B]): SparkStream[F, B] =
    new SparkStream[F, B](typedDataset.deserialized.flatMap(f).dataset, params)

  // sinks

  def consoleSink: NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](ds.writeStream, params)

  def fileSink(path: String): NJFileSink[F, A] =
    new NJFileSink[F, A](ds.writeStream, params, path)
}
