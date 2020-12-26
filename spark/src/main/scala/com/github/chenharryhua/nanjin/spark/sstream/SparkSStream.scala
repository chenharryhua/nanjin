package com.github.chenharryhua.nanjin.spark.sstream

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.spark.SparkDatetimeConversionConstant
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Dataset, Row}

final class SparkSStream[F[_], A](ds: Dataset[A], cfg: SStreamConfig) extends Serializable {
  val params: SStreamParams = cfg.evalConfig

  private def updateConfig(f: SStreamConfig => SStreamConfig): SparkSStream[F, A] =
    new SparkSStream[F, A](ds, f(cfg))

  def checkpoint(cp: String): SparkSStream[F, A]     = updateConfig(_.withCheckpointReplace(cp))
  def failOnDataLoss: SparkSStream[F, A]             = updateConfig(_.failOnDataLoss)
  def ignoreDataLoss: SparkSStream[F, A]             = updateConfig(_.ignoreDataLoss)
  def trigger(trigger: Trigger): SparkSStream[F, A]  = updateConfig(_.withTrigger(trigger))
  def progressInterval(ms: Long): SparkSStream[F, A] = updateConfig(_.withProgressInterval(ms))

  // transforms

  def transform[B](f: Dataset[A] => Dataset[B]): SparkSStream[F, B] =
    new SparkSStream[F, B](ds.transform(f), cfg)

  def filter(f: A => Boolean): SparkSStream[F, A] = transform(_.filter(f))

  def map[B: TypedEncoder](f: A => B): SparkSStream[F, B] =
    transform(_.map(f)(TypedExpressionEncoder[B]))

  def flatMap[B: TypedEncoder](f: A => TraversableOnce[B]): SparkSStream[F, B] =
    transform(_.flatMap(f)(TypedExpressionEncoder[B]))

  // sinks

  def consoleSink: NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](ds.writeStream, cfg)

  def fileSink(path: String): NJFileSink[F, A] =
    new NJFileSink[F, A](ds.writeStream, cfg, path)

  def memorySink(queryName: String): NJMemorySink[F, A] =
    new NJMemorySink[F, A](ds.writeStream, cfg, queryName)

  def datePartitionSink(path: String): NJFileSink[F, Row] = {
    val ts = (col("timestamp") / SparkDatetimeConversionConstant).cast(TimestampType)
    val ws = ds
      .withColumn("Year", year(ts))
      .withColumn("Month", format_string("%02d", month(ts)))
      .withColumn("Day", format_string("%02d", dayofmonth(ts)))
      .writeStream
    new NJFileSink[F, Row](ws, cfg, path).partitionBy("Year", "Month", "Day")
  }
}
