package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQueryProgress, Trigger}

final class NJMemorySink[F[_], A](dsw: DataStreamWriter[A], cfg: SStreamConfig) extends NJStreamSink[F] {

  override val params: SStreamParams = cfg.evalConfig

  private def updateCfg(f: SStreamConfig => SStreamConfig): NJMemorySink[F, A] =
    new NJMemorySink[F, A](dsw, f(cfg))

  // https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
  def append: NJMemorySink[F, A]   = updateCfg(_.withAppend)
  def complete: NJMemorySink[F, A] = updateCfg(_.withComplete)

  def trigger(trigger: Trigger): NJMemorySink[F, A] = updateCfg(_.withTrigger(trigger))

  override def queryStream(implicit F: Concurrent[F], timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .trigger(params.trigger)
        .format("memory")
        .queryName(params.queryName.getOrElse("memory"))
        .outputMode(params.outputMode)
        .option("failOnDataLoss", params.dataLoss.value),
      params.progressInterval
    )
}
