package com.github.chenharryhua.nanjin.spark.streaming

import cats.effect.{Concurrent, Timer}
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.NJPath
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}

@Lenses final case class NJFileSink[F[_], A](
  dsw: DataStreamWriter[A],
  fileFormat: NJFileFormat,
  path: NJPath,
  checkpoint: NJCheckpoint,
  dataLoss: NJFailOnDataLoss,
  trigger: Trigger)
    extends NJStreamSink[F] {

  // settings
  def withTrigger(tg: Trigger): NJFileSink[F, A] =
    NJFileSink.trigger.set(tg)(this)

  def withFileFormat(fm: NJFileFormat): NJFileSink[F, A] =
    NJFileSink.fileFormat.set(fm)(this)

  def withJson: NJFileSink[F, A]    = withFileFormat(NJFileFormat.Json)
  def withAvro: NJFileSink[F, A]    = withFileFormat(NJFileFormat.Avro)
  def withParquet: NJFileSink[F, A] = withFileFormat(NJFileFormat.Parquet)

  def withCheckpoint(cp: String): NJFileSink[F, A] =
    NJFileSink.checkpoint.set(NJCheckpoint(cp))(this)

  def withoutFailONDataLoss: NJFileSink[F, A] =
    NJFileSink.dataLoss.set(NJFailOnDataLoss(false))(this)

  // ops
  def withOptions(f: DataStreamWriter[A] => DataStreamWriter[A]): NJFileSink[F, A] =
    NJFileSink.dsw[F, A].modify(f)(this)

  def partitionBy(colNames: String*): NJFileSink[F, A] =
    NJFileSink.dsw[F, A].modify(_.partitionBy(colNames: _*))(this)

  override def run(implicit F: Concurrent[F], timer: Timer[F]): F[Unit] =
    ss.queryStream(
        dsw
          .trigger(trigger)
          .format(fileFormat.format)
          .outputMode(OutputMode.Append)
          .option("path", path.value)
          .option("checkpointLocation", checkpoint.value)
          .option("failOnDataLoss", dataLoss.value))
      .compile
      .drain
}
