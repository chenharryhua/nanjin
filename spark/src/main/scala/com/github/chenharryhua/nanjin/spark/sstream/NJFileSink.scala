package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.kernel.Async
import cats.Endo
import com.github.chenharryhua.nanjin.terminals.NJPath
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress, Trigger}

import scala.concurrent.duration.FiniteDuration

final class NJFileSink[F[_], A](dsw: DataStreamWriter[A], cfg: SStreamConfig, path: NJPath)
    extends NJStreamSink[F] {

  override val params: SStreamParams = cfg.evalConfig

  private def updateCfg(f: Endo[SStreamConfig]): NJFileSink[F, A] =
    new NJFileSink[F, A](dsw, f(cfg), path)

  def parquet: NJFileSink[F, A] = updateCfg(_.parquetFormat)
  def avro: NJFileSink[F, A]    = updateCfg(_.avroFormat)

  def triggerEvery(duration: FiniteDuration): NJFileSink[F, A] =
    updateCfg(_.triggerMode(Trigger.ProcessingTime(duration)))

  def withOptions(f: Endo[DataStreamWriter[A]]): NJFileSink[F, A] =
    new NJFileSink(f(dsw), cfg, path)

  def queryName(name: String): NJFileSink[F, A] = updateCfg(_.queryName(name))

  def partitionBy(colNames: String*): NJFileSink[F, A] =
    new NJFileSink[F, A](dsw.partitionBy(colNames*), cfg, path)

  override def stream(implicit F: Async[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .trigger(params.trigger)
        .format(params.fileFormat.format)
        .queryName(params.queryName.getOrElse(path.pathStr))
        .outputMode(OutputMode.Append)
        .option("path", path.pathStr)
        .option("checkpointLocation", params.checkpoint.pathStr)
        .option("failOnDataLoss", params.dataLoss.value),
      params.progressInterval
    )
}
