package com.github.chenharryhua.nanjin.spark.sstream

import cats.Endo
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import frameless.{TypedEncoder, TypedExpressionEncoder}
import io.lemonlabs.uri.Url
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, Row}

final class SparkSStream[F[_], A](val dataset: Dataset[A], cfg: SStreamConfig) extends Serializable {
  val params: SStreamParams = cfg.evalConfig

  private def updateConfig(f: Endo[SStreamConfig]): SparkSStream[F, A] =
    new SparkSStream[F, A](dataset, f(cfg))

  def checkpoint(cp: Url): SparkSStream[F, A] = updateConfig(_.checkpoint(cp))
  def failOnDataLoss: SparkSStream[F, A] = updateConfig(_.dataLossFailure)
  def ignoreDataLoss: SparkSStream[F, A] = updateConfig(_.dataLossIgnore)

  def progressInterval(ms: Long): SparkSStream[F, A] = updateConfig(_.progressInterval(ms))

  // transforms

  def transform[B](f: Dataset[A] => Dataset[B]): SparkSStream[F, B] =
    new SparkSStream[F, B](dataset.transform(f), cfg)

  def filter(f: A => Boolean): SparkSStream[F, A] = transform(_.filter(f))

  def map[B: TypedEncoder](f: A => B): SparkSStream[F, B] =
    transform(_.map(f)(TypedExpressionEncoder[B]))

  def flatMap[B: TypedEncoder](f: A => IterableOnce[B]): SparkSStream[F, B] =
    transform(_.flatMap(f)(TypedExpressionEncoder[B]))

  def coalesce: SparkSStream[F, A] = transform(_.coalesce(1))

  // sinks

  def consoleSink: SparkConsoleSink[F, A] =
    new SparkConsoleSink[F, A](dataset.writeStream, cfg)

  def fileSink(path: Url): SparkFileSink[F, A] =
    new SparkFileSink[F, A](dataset.writeStream, cfg, path)

  def memorySink(queryName: String): SparkMemorySink[F, A] =
    new SparkMemorySink[F, A](dataset.writeStream, cfg.queryName(queryName))

  def datePartitionSink(path: Url): SparkFileSink[F, Row] = {
    val year = udf((ts: Long) => NJTimestamp(ts).atZone(params.zoneId).toLocalDate.getYear)
    val month = udf((ts: Long) => NJTimestamp(ts).atZone(params.zoneId).toLocalDate.getMonthValue)
    val day = udf((ts: Long) => NJTimestamp(ts).atZone(params.zoneId).toLocalDate.getDayOfMonth)

    val ws = dataset
      .withColumn("Year", year(dataset("timestamp")))
      .withColumn("Month", month(dataset("timestamp")))
      .withColumn("Day", day(dataset("timestamp")))
      .writeStream
    new SparkFileSink[F, Row](ws, cfg, path).partitionBy("Year", "Month", "Day")
  }
}
