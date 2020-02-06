package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJFailOnDataLoss, NJShowDataset}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

trait NJStreamSink[F[_]] extends Serializable {
  def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit]
}

final class NJConsoleSink[F[_], A](
  dsw: DataStreamWriter[A],
  showDs: NJShowDataset,
  dataLoss: NJFailOnDataLoss)
    extends NJStreamSink[F] {

  def withoutFailONDataLoss: NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](dsw, showDs, NJFailOnDataLoss(false))

  def withShowRows(rs: Int): NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](dsw, NJShowDataset.rowNum.set(rs)(showDs), dataLoss)

  def withTruncate: NJConsoleSink[F, A] =
    new NJConsoleSink[F, A](dsw, NJShowDataset.isTruncate.set(true)(showDs), dataLoss)

  override def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    ss.queryStream(
        dsw
          .format("console")
          .outputMode(OutputMode.Append)
          .option("truncate", showDs.isTruncate.toString)
          .option("numRows", showDs.rowNum.toString)
          .option("failOnDataLoss", dataLoss.value))
      .compile
      .drain
}
