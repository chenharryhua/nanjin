package com.github.chenharryhua.nanjin.spark.streaming

import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.Dataset

final class SparkStreamTransformer[F[_], A: TypedEncoder](ds: Dataset[A]) extends Serializable {
  @transient lazy val typedDataset: TypedDataset[A] = TypedDataset.create(ds)

  def filter(f: A => Boolean): SparkStreamTransformer[F, A] =
    new SparkStreamTransformer[F, A](ds.filter(f))

  def map[B: TypedEncoder](f: A => B): SparkStreamTransformer[F, B] =
    new SparkStreamTransformer[F, B](typedDataset.deserialized.map(f).dataset)

  def flatMap[B: TypedEncoder](f: A => TraversableOnce[B]) =
    new SparkStreamTransformer[F, B](typedDataset.deserialized.flatMap(f).dataset)

  def withSink(sink: StreamOutputSink) = new SparkStreamRunner[F, A](ds.writeStream, sink)

}
