package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import org.apache.spark.sql.streaming.{
  DataStreamWriter,
  OutputMode,
  StreamingQueryProgress,
  Trigger
}

final class NJConsoleSink[F[_], A](dsw: DataStreamWriter[A], cfg: SStreamConfig)
    extends NJStreamSink[F] {

  override val params: SStreamParams = cfg.evalConfig

  private def updateConfig(f: SStreamConfig => SStreamConfig): NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](dsw, f(cfg))

  def rows(num: Int): NJConsoleSink[F, A] = updateConfig(_.withShowRows(num))
  def truncate: NJConsoleSink[F, A]       = updateConfig(_.withShowTruncate(true))
  def untruncate: NJConsoleSink[F, A]     = updateConfig(_.withShowTruncate(false))

  def trigger(trigger: Trigger): NJConsoleSink[F, A] = updateConfig(_.withTrigger(trigger))

  override def queryStream(implicit
    F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .trigger(params.trigger)
        .format("console")
        .outputMode(OutputMode.Append)
        .option("truncate", params.showDs.isTruncate.toString)
        .option("numRows", params.showDs.rowNum.toString)
        .option("failOnDataLoss", params.dataLoss.value),
      params.progressInterval
    )

}
