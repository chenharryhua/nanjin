package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJFailOnDataLoss, NJShowDataset}
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.OutputMode

@Lenses final case class StreamParams(
  showDs: NJShowDataset,
  checkpoint: NJCheckpoint,
  fileFormat: NJFileFormat,
  dataLoss: NJFailOnDataLoss,
  outputMode: OutputMode) {

  def withFileFormat(fileFormat: NJFileFormat): StreamParams =
    StreamParams.fileFormat.set(fileFormat)(this)

  def withCheckpoint(cp: String): StreamParams =
    StreamParams.checkpoint.set(NJCheckpoint(cp))(this)

  def withMode(mode: OutputMode): StreamParams =
    StreamParams.outputMode.set(mode)(this)

  def withFailOnDataLoss: StreamParams =
    StreamParams.dataLoss.set(NJFailOnDataLoss(true))(this)

  def withoutFailOnDataLoss: StreamParams =
    StreamParams.dataLoss.set(NJFailOnDataLoss(false))(this)

}

object StreamParams {

  def default: StreamParams =
    StreamParams(
      NJShowDataset(20, isTruncate = false),
      NJCheckpoint("./data/streaming/checkpoint"),
      NJFileFormat.Json,
      NJFailOnDataLoss(true),
      OutputMode.Append)
}
