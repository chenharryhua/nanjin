package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.kernel.Async
import cats.Endo
import com.github.chenharryhua.nanjin.common.utils.random4d
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQueryProgress, Trigger}

final class SparkMemorySink[F[_], A](dsw: DataStreamWriter[A], cfg: SStreamConfig)
    extends SparkStreamSink[F] {

  override val params: SStreamParams = cfg.evalConfig

  private def updateCfg(f: Endo[SStreamConfig]): SparkMemorySink[F, A] =
    new SparkMemorySink[F, A](dsw, f(cfg))

  // https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
  def append: SparkMemorySink[F, A] = updateCfg(_.appendMode)
  def complete: SparkMemorySink[F, A] = updateCfg(_.completeMode)

  def trigger(trigger: Trigger): SparkMemorySink[F, A] = updateCfg(_.triggerMode(trigger))

  override def stream(implicit F: Async[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .trigger(params.trigger)
        .format("memory")
        .queryName(params.queryName.getOrElse(s"memory-${random4d.value}"))
        .outputMode(params.outputMode)
        .option("failOnDataLoss", params.dataLoss.value),
      params.progressInterval
    )
}
