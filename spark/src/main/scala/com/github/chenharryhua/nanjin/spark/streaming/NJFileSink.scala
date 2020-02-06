package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJFailOnDataLoss, NJPath}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

final class NJFileSink[F[_], A](
  dsw: DataStreamWriter[A],
  fileFormat: NJFileFormat,
  path: NJPath,
  checkpoint: NJCheckpoint,
  dataLoss: NJFailOnDataLoss)
    extends NJStreamSink[F] {

  def withJson: NJFileSink[F, A] =
    new NJFileSink[F, A](dsw, NJFileFormat.Json, path, checkpoint, dataLoss)

  def withAvro: NJFileSink[F, A] =
    new NJFileSink[F, A](dsw, NJFileFormat.Avro, path, checkpoint, dataLoss)

  def withParquet: NJFileSink[F, A] =
    new NJFileSink[F, A](dsw, NJFileFormat.Parquet, path, checkpoint, dataLoss)

  def withCheckpoint(cp: String): NJFileSink[F, A] =
    new NJFileSink[F, A](dsw, fileFormat, path, NJCheckpoint(cp), dataLoss)

  def withoutFailONDataLoss: NJFileSink[F, A] =
    new NJFileSink[F, A](dsw, fileFormat, path, checkpoint, NJFailOnDataLoss(false))

  def withOptions(f: DataStreamWriter[A] => DataStreamWriter[A]) =
    new NJFileSink[F, A](f(dsw), fileFormat, path, checkpoint, dataLoss)

  def partitionBy(colNames: String*): NJFileSink[F, A] =
    new NJFileSink[F, A](dsw.partitionBy(colNames: _*), fileFormat, path, checkpoint, dataLoss)

  override def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    ss.queryStream(
        dsw
          .format(fileFormat.format)
          .outputMode(OutputMode.Append)
          .option("path", path.value)
          .option("checkpointLocation", checkpoint.value)
          .option("failOnDataLoss", dataLoss.value))
      .compile
      .drain
}
