package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJFailOnDataLoss, NJPath}
import org.apache.spark.sql.streaming.OutputMode
import shapeless._

final class StreamParams[HL <: HList](val hl: HL) {

  def withPath(path: String): StreamParams[NJPath :: HL] =
    new StreamParams(NJPath(path) :: hl)

  def withFileFormat(fileFormat: NJFileFormat): StreamParams[NJFileFormat :: HL] =
    new StreamParams(fileFormat :: hl)

  def withCheckpoint(checkpoint: String): StreamParams[NJCheckpoint :: HL] =
    new StreamParams(NJCheckpoint(checkpoint) :: hl)

  def withMode(mode: OutputMode): StreamParams[OutputMode :: HL] =
    new StreamParams(mode :: hl)

  def withFailOnDataLoss: StreamParams[NJFailOnDataLoss :: HL] =
    new StreamParams(NJFailOnDataLoss(true) :: hl)

  def withoutFailOnDataLoss: StreamParams[NJFailOnDataLoss :: HL] =
    new StreamParams(NJFailOnDataLoss(false) :: hl)

}

object StreamParams {
  def empty: StreamParams[HList] = new StreamParams[HList](HNil)
}
