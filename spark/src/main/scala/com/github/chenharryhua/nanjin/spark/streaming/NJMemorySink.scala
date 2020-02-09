package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress}

final class NJMemorySink[F[_], A](
  dsw: DataStreamWriter[A],
  mode: OutputMode,
  queryName: String,
  dataLoss: NJFailOnDataLoss)
    extends NJStreamSink[F] {

  override def queryStream(
    implicit F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .format("memory")
        .queryName(queryName)
        .outputMode(mode)
        .option("failOnDataLoss", dataLoss.value))

}
