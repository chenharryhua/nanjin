package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQueryProgress, Trigger}

final class NJMemorySink[F[_], A](dsw: DataStreamWriter[A], cfg: SStreamConfig, queryName: String)
    extends NJStreamSink[F] {

  override val params: SStreamParams = cfg.evalConfig

  private def updateConfig(f: SStreamConfig => SStreamConfig): NJMemorySink[F, A] =
    new NJMemorySink[F, A](dsw, f(cfg), queryName)

  def append: NJMemorySink[F, A] = updateConfig(_.withAppend)
  def update: NJMemorySink[F, A] = updateConfig(_.withUpdate)

  override def queryStream(implicit
    F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .trigger(params.trigger)
        .format("memory")
        .queryName(queryName)
        .outputMode(params.outputMode)
        .option("failOnDataLoss", params.dataLoss.value),
      params.progressInterval
    )
}
