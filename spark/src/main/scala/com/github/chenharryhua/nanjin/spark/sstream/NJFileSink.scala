package com.github.chenharryhua.nanjin.spark.sstream

import cats.effect.kernel.Async
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress, Trigger}

import scala.concurrent.duration.FiniteDuration

final class NJFileSink[F[_], A](dsw: DataStreamWriter[A], cfg: SStreamConfig, path: String) extends NJStreamSink[F] {

  override val params: SStreamParams = cfg.evalConfig

  private def updateCfg(f: SStreamConfig => SStreamConfig): NJFileSink[F, A] =
    new NJFileSink[F, A](dsw, f(cfg), path)

  def parquet: NJFileSink[F, A] = updateCfg(_.parquet_format)
  def json: NJFileSink[F, A]    = updateCfg(_.json_format)
  def avro: NJFileSink[F, A]    = updateCfg(_.avro_format)

  def triggerEvery(duration: FiniteDuration): NJFileSink[F, A] =
    updateCfg(_.trigger_mode(Trigger.ProcessingTime(duration)))

  def withOptions(f: DataStreamWriter[A] => DataStreamWriter[A]): NJFileSink[F, A] =
    new NJFileSink(f(dsw), cfg, path)

  def queryName(name: String): NJFileSink[F, A] = updateCfg(_.query_name(name))

  def partitionBy(colNames: String*): NJFileSink[F, A] =
    new NJFileSink[F, A](dsw.partitionBy(colNames: _*), cfg, path)

  override def queryStream(implicit F: Async[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .trigger(params.trigger)
        .format(params.fileFormat.format)
        .queryName(params.queryName.getOrElse(path))
        .outputMode(OutputMode.Append)
        .option("path", path)
        .option("checkpointLocation", params.checkpoint)
        .option("failOnDataLoss", params.dataLoss.value),
      params.progressInterval
    )
}
