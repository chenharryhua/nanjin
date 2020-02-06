package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.spark.{NJFailOnDataLoss, NJShowDataset}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

final class NJConsoleSink[F[_], A](
  dsw: DataStreamWriter[A],
  showDs: NJShowDataset,
  dataLoss: NJFailOnDataLoss) {

  def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
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
