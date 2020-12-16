package com.github.chenharryhua.nanjin.spark.sstream

import com.github.chenharryhua.nanjin.spark.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.sstream
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.Dataset

final class SparkSStream[F[_], A](ds: Dataset[A], cfg: SStreamConfig) extends Serializable {

  val params: SStreamParams = cfg.evalConfig

  def withParamUpdate(f: SStreamConfig => SStreamConfig): SparkSStream[F, A] =
    new SparkSStream[F, A](ds, f(cfg))

  // transforms

  def filter(f: A => Boolean): SparkSStream[F, A] =
    new SparkSStream[F, A](ds.filter(f), cfg)

  def map[B: TypedEncoder](f: A => B): SparkSStream[F, B] =
    new SparkSStream[F, B](ds.map(f)(TypedExpressionEncoder[B]), cfg)

  def flatMap[B: TypedEncoder](f: A => TraversableOnce[B]): SparkSStream[F, B] =
    new SparkSStream[F, B](ds.flatMap(f)(TypedExpressionEncoder[B]), cfg)

  def transform[B: TypedEncoder](f: Dataset[A] => Dataset[B]) =
    new SparkSStream[F, B](f(ds), cfg)

  // sinks

  def consoleSink: NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](ds.writeStream, cfg)

  def fileSink(path: String): NJFileSink[F, A] =
    new NJFileSink[F, A](ds.writeStream, cfg, path)

  def datePartitionFileSink[K: TypedEncoder, V: TypedEncoder](path: String)(implicit
    ev: A =:= OptionalKV[K, V]): NJFileSink[F, DatePartitionedCR[K, V]] = {
    implicit val te: TypedEncoder[DatePartitionedCR[K, V]] = shapeless.cachedImplicit
    new NJFileSink[F, DatePartitionedCR[K, V]](
      ds.map(x => DatePartitionedCR(params.timeRange.zoneId)(ev(x)))(
        TypedExpressionEncoder[sstream.DatePartitionedCR[K, V]])
        .writeStream,
      cfg,
      path).partitionBy("Year", "Month", "Day")
  }
}
