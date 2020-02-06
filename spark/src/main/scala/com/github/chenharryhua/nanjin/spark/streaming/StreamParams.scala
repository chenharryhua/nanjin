package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.NJShowDataset
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.OutputMode

final private[spark] case class NJCheckpoint(value: String) extends AnyVal
final private[spark] case class NJFailOnDataLoss(value: Boolean) extends AnyVal

@Lenses final case class StreamParams(
  showDs: NJShowDataset,
  fileFormat: NJFileFormat,
  dataLoss: NJFailOnDataLoss,
  outputMode: OutputMode) {

  def withFileFormat(fileFormat: NJFileFormat): StreamParams =
    StreamParams.fileFormat.set(fileFormat)(this)

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
      NJFileFormat.Json,
      NJFailOnDataLoss(true),
      OutputMode.Append)
}
