package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.spark.NJShowDataset
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}

trait NJStreamSink[F[_]] extends Serializable {
  def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit]
}

@Lenses final case class NJConsoleSink[F[_], A](
  dataStreamWriter: DataStreamWriter[A],
  showDs: NJShowDataset,
  dataLoss: NJFailOnDataLoss,
  trigger: Trigger)
    extends NJStreamSink[F] {

  def withTrigger(tg: Trigger): NJConsoleSink[F, A] =
    NJConsoleSink.trigger.set(tg)(this)

  def withoutFailONDataLoss: NJConsoleSink[F, A] =
    NJConsoleSink.dataLoss.set(NJFailOnDataLoss(false))(this)

  def withShowRows(rs: Int): NJConsoleSink[F, A] =
    NJConsoleSink.showDs.composeLens(NJShowDataset.rowNum).set(rs)(this)

  def withTruncate: NJConsoleSink[F, A] =
    NJConsoleSink.showDs.composeLens(NJShowDataset.isTruncate).set(true)(this)

  override def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    ss.queryStream(
        dataStreamWriter
          .trigger(trigger)
          .format("console")
          .outputMode(OutputMode.Append)
          .option("truncate", showDs.isTruncate.toString)
          .option("numRows", showDs.rowNum.toString)
          .option("failOnDataLoss", dataLoss.value))
      .compile
      .drain
}
