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
  failOnDataLoss: NJFailOnDataLoss) {

  def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    ss.queryStream(
        dsw
          .format(fileFormat.format)
          .outputMode(OutputMode.Append)
          .option("path", path.value)
          .option("checkpointLocation", checkpoint.value)
          .option("failOnDataLoss", failOnDataLoss.value))
      .compile
      .drain
}
