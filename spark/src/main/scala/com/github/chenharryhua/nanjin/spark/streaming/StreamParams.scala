package com.github.chenharryhua.nanjin.spark.streaming

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.spark.{NJCheckpoint, NJPath}
import shapeless._
import shapeless.ops.hlist.Selector

final class StreamParams[HL <: HList](hl: HL) {

  def withPath(path: String): StreamParams[NJPath :: HL] =
    new StreamParams(NJPath(path) :: hl)

  def withFileFormat(fileFormat: NJFileFormat): StreamParams[NJFileFormat :: HL] =
    new StreamParams(fileFormat :: hl)

  def withCheckpoint(checkpoint: String): StreamParams[NJCheckpoint :: HL] =
    new StreamParams(NJCheckpoint(checkpoint) :: hl)

  def withStreamMode(mode: StreamOutputMode): StreamParams[StreamOutputMode :: HL] =
    new StreamParams(mode :: hl)

  def fileSink(
    implicit
    fileFormat: Selector[HL, NJFileFormat],
    path: Selector[HL, NJPath],
    checkpoint: Selector[HL, NJCheckpoint]): FileSink =
    FileSink.append(fileFormat(hl), path(hl), checkpoint(hl))
}
