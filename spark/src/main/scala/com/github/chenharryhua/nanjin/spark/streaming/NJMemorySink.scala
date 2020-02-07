package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

final class NJMemorySink[F[_], A](
  dsw: DataStreamWriter[A],
  mode: OutputMode,
  queryName: String,
  dataLoss: NJFailOnDataLoss)
    extends NJStreamSink[F] {

  override def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    ss.queryStream(
        dsw
          .format("memory")
          .queryName(queryName)
          .outputMode(mode)
          .option("failOnDataLoss", dataLoss.value))
      .compile
      .drain

}
