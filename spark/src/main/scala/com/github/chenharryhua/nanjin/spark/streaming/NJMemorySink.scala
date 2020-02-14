package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQueryProgress}

final class NJMemorySink[F[_], A](
  dsw: DataStreamWriter[A],
  params: StreamConfigF.StreamConfig,
  queryName: String)
    extends NJStreamSink[F] {

  private val p: StreamParams = StreamConfigF.evalParams(params)

  override def queryStream(
    implicit F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .format("memory")
        .queryName(queryName)
        .outputMode(p.outputMode)
        .option("failOnDataLoss", p.dataLoss.value))

}
