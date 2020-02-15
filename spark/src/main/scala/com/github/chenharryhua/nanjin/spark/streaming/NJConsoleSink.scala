package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQueryProgress}

final class NJConsoleSink[F[_], A](dsw: DataStreamWriter[A], cfg: StreamConfig)
    extends NJStreamSink[F] {

  private val p: StreamParams = StreamConfigF.evalParams(cfg)

  override def queryStream(
    implicit F: Concurrent[F],
    timer: Timer[F]): Stream[F, StreamingQueryProgress] =
    ss.queryStream(
      dsw
        .trigger(p.trigger)
        .format("console")
        .outputMode(OutputMode.Append)
        .option("truncate", p.showDs.isTruncate.toString)
        .option("numRows", p.showDs.rowNum.toString)
        .option("failOnDataLoss", p.dataLoss.value))

}
