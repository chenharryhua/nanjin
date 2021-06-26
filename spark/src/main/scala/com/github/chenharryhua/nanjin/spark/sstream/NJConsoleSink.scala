package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.Async
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.utils.random4d
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress, Trigger}

final class NJConsoleSink[F[_], A](
  dsw: DataStreamWriter[A],
  cfg: SStreamConfig,
  numRows: Int = 20,
  isTruncate: Boolean = false)
    extends NJStreamSink[F] with UpdateConfig[SStreamConfig, NJConsoleSink[F, A]] {

  override val params: SStreamParams = cfg.evalConfig

  def rows(num: Int): NJConsoleSink[F, A] = new NJConsoleSink[F, A](dsw, cfg, num, isTruncate)
  def truncate: NJConsoleSink[F, A]       = new NJConsoleSink[F, A](dsw, cfg, numRows, true)
  def untruncate: NJConsoleSink[F, A]     = new NJConsoleSink[F, A](dsw, cfg, numRows, false)

  override def updateConfig(f: SStreamConfig => SStreamConfig): NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](dsw, f(cfg), numRows, isTruncate)

  def trigger(trigger: Trigger): NJConsoleSink[F, A] = updateConfig(_.withTrigger(trigger))
  // https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
  def append: NJConsoleSink[F, A]                  = updateConfig(_.withAppend)
  def update: NJConsoleSink[F, A]                  = updateConfig(_.withUpdate)
  def complete: NJConsoleSink[F, A]                = updateConfig(_.withComplete)
  def queryName(name: String): NJConsoleSink[F, A] = updateConfig(_.withQueryName(name))

  override def queryStream(implicit F: Async[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
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
