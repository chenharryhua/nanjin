package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.NJShowDataset
import monocle.macros.Lenses
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

final private[spark] case class NJCheckpoint(value: String) extends AnyVal {

  def append(sub: String): NJCheckpoint = {
    val s = if (sub.startsWith("/")) sub.tail else sub
    val v = if (value.endsWith("/")) value.dropRight(1) else value
    NJCheckpoint(s"$v/$s")
  }
}

final private[spark] case class NJFailOnDataLoss(value: Boolean) extends AnyVal

@Lenses final case class StreamParams(
  checkpoint: NJCheckpoint,
  showDs: NJShowDataset,
  fileFormat: NJFileFormat,
  dataLoss: NJFailOnDataLoss,
  outputMode: OutputMode,
  trigger: Trigger) {

  def withFileFormat(fileFormat: NJFileFormat): StreamParams =
    StreamParams.fileFormat.set(fileFormat)(this)

  def withMode(mode: OutputMode): StreamParams =
    StreamParams.outputMode.set(mode)(this)

  def withFailOnDataLoss: StreamParams =
    StreamParams.dataLoss.set(NJFailOnDataLoss(true))(this)

  def withoutFailOnDataLoss: StreamParams =
    StreamParams.dataLoss.set(NJFailOnDataLoss(false))(this)

  def withCheckpoint(cp: NJCheckpoint): StreamParams =
    StreamParams.checkpoint.set(cp)(this)

  def withTrigger(tg: Trigger): StreamParams =
    StreamParams.trigger.set(tg)(this)

}

object StreamParams {

  def apply(checkpoint: String): StreamParams =
    StreamParams(
      NJCheckpoint(checkpoint),
      NJShowDataset(20, isTruncate = false),
      NJFileFormat.Json,
      NJFailOnDataLoss(true),
      OutputMode.Append,
      Trigger.ProcessingTime(0))
}
