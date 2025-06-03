package com.github.chenharryhua.nanjin.spark.sstream

import cats.Endo
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.terminals.toHadoopPath
import fs2.Stream
import io.lemonlabs.uri.Url
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress, Trigger}

import scala.concurrent.duration.FiniteDuration

final class SparkFileSink[F[_], A](dsw: DataStreamWriter[A], cfg: SStreamConfig, path: Url)
    extends SparkStreamSink[F] {

  override val params: SStreamParams = cfg.evalConfig

  private def updateCfg(f: Endo[SStreamConfig]): SparkFileSink[F, A] =
    new SparkFileSink[F, A](dsw, f(cfg), path)

  def parquet: SparkFileSink[F, A] = updateCfg(_.parquetFormat)
  def avro: SparkFileSink[F, A] = updateCfg(_.avroFormat)

  def triggerEvery(duration: FiniteDuration): SparkFileSink[F, A] =
    updateCfg(_.triggerMode(Trigger.ProcessingTime(duration)))

  def withOptions(f: Endo[DataStreamWriter[A]]): SparkFileSink[F, A] =
    new SparkFileSink(f(dsw), cfg, path)

  def queryName(name: String): SparkFileSink[F, A] = updateCfg(_.queryName(name))

  def partitionBy(colNames: String*): SparkFileSink[F, A] =
    new SparkFileSink[F, A](dsw.partitionBy(colNames*), cfg, path)

  override def stream(implicit F: Async[F]): Stream[F, StreamingQueryProgress] = {
    val ps = toHadoopPath(path).toString
    ss.queryStream(
      dsw
        .trigger(params.trigger)
        .format(params.fileFormat.format)
        .queryName(params.queryName.getOrElse(ps))
        .outputMode(OutputMode.Append)
        .option("path", ps)
        .option("checkpointLocation", toHadoopPath(params.checkpoint).toString)
        .option("failOnDataLoss", params.dataLoss.value),
      params.progressInterval
    )
  }
}
