package com.github.chenharryhua.nanjin.spark.sstream

import cats.Endo
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.utils.random4d
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress, Trigger}

final class SparkConsoleSink[F[_], A](
  dsw: DataStreamWriter[A],
  cfg: SStreamConfig,
  numRows: Int = 20,
  isTruncate: Boolean = false)
    extends SparkStreamSink[F] with UpdateConfig[SStreamConfig, SparkConsoleSink[F, A]] {

  override val params: SStreamParams = cfg.evalConfig

  def rows(num: Int): SparkConsoleSink[F, A] = new SparkConsoleSink[F, A](dsw, cfg, num, isTruncate)
  def truncate: SparkConsoleSink[F, A] = new SparkConsoleSink[F, A](dsw, cfg, numRows, true)
  def untruncate: SparkConsoleSink[F, A] = new SparkConsoleSink[F, A](dsw, cfg, numRows, false)

  override def updateConfig(f: Endo[SStreamConfig]): SparkConsoleSink[F, A] =
    new SparkConsoleSink[F, A](dsw, f(cfg), numRows, isTruncate)

  def trigger(trigger: Trigger): SparkConsoleSink[F, A] = updateConfig(_.triggerMode(trigger))
  // https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
  def append: SparkConsoleSink[F, A] = updateConfig(_.appendMode)
  def update: SparkConsoleSink[F, A] = updateConfig(_.updateMode)
  def complete: SparkConsoleSink[F, A] = updateConfig(_.completeMode)
  def queryName(name: String): SparkConsoleSink[F, A] = updateConfig(_.queryName(name))

  override def stream(implicit F: Async[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream[F, A](
      dsw
        .trigger(params.trigger)
        .format("console")
        .queryName(params.queryName.getOrElse(s"console-${random4d.value}"))
        .outputMode(OutputMode.Append)
        .option("numRows", numRows.toString)
        .option("truncate", isTruncate.toString)
        .option("failOnDataLoss", params.dataLoss.value),
      params.progressInterval
    )
}
