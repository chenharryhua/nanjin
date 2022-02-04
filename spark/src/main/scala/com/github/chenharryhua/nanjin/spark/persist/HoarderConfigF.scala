package com.github.chenharryhua.nanjin.spark.persist

import cats.Functor
import com.github.chenharryhua.nanjin.common.{ChunkSize, NJFileFormat}
import com.github.chenharryhua.nanjin.terminals.NJPath
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import squants.information.{Information, Megabytes}

@Lenses final private[persist] case class HoarderParams(
  format: NJFileFormat,
  outPath: NJPath,
  saveMode: SaveMode,
  compression: NJCompression,
  chunkSize: ChunkSize,
  byteBuffer: Information)

private[persist] object HoarderParams {

  def apply(outPath: NJPath): HoarderParams =
    HoarderParams(
      NJFileFormat.Unknown,
      outPath,
      SaveMode.Overwrite,
      NJCompression.Uncompressed,
      ChunkSize(1000),
      Megabytes(1))
}

sealed private[persist] trait HoarderConfigF[X]

private object HoarderConfigF {
  implicit val functorHoarderConfigF: Functor[HoarderConfigF] = cats.derived.semiauto.functor[HoarderConfigF]

  final case class InitParams[K](path: NJPath) extends HoarderConfigF[K]
  final case class WithSaveMode[K](value: SaveMode, cont: K) extends HoarderConfigF[K]
  final case class WithOutputPath[K](value: NJPath, cont: K) extends HoarderConfigF[K]
  final case class WithFileFormat[K](value: NJFileFormat, cont: K) extends HoarderConfigF[K]
  final case class WithCompression[K](value: NJCompression, cont: K) extends HoarderConfigF[K]

  final case class WithChunkSize[K](value: ChunkSize, cont: K) extends HoarderConfigF[K]
  final case class WithByteBuffer[K](value: Information, cont: K) extends HoarderConfigF[K]

  private val algebra: Algebra[HoarderConfigF, HoarderParams] =
    Algebra[HoarderConfigF, HoarderParams] {
      case InitParams(v)         => HoarderParams(v)
      case WithSaveMode(v, c)    => HoarderParams.saveMode.set(v)(c)
      case WithOutputPath(v, c)  => HoarderParams.outPath.set(v)(c)
      case WithFileFormat(v, c)  => HoarderParams.format.set(v)(c)
      case WithCompression(v, c) => HoarderParams.compression.set(v)(c)
      case WithChunkSize(v, c)   => HoarderParams.chunkSize.set(v)(c)
      case WithByteBuffer(v, c)  => HoarderParams.byteBuffer.set(v)(c)
    }

  def evalConfig(cfg: HoarderConfig): HoarderParams = scheme.cata(algebra).apply(cfg.value)
}

final private[spark] case class HoarderConfig(value: Fix[HoarderConfigF]) {
  import HoarderConfigF.*
  val evalConfig: HoarderParams = HoarderConfigF.evalConfig(this)

  def saveMode(sm: SaveMode): HoarderConfig =
    HoarderConfig(Fix(WithSaveMode(sm, value)))

  def errorMode: HoarderConfig     = saveMode(SaveMode.ErrorIfExists)
  def ignoreMode: HoarderConfig    = saveMode(SaveMode.Ignore)
  def overwriteMode: HoarderConfig = saveMode(SaveMode.Overwrite)
  def appendMode: HoarderConfig    = saveMode(SaveMode.Append)

  def outputPath(outPath: NJPath): HoarderConfig =
    HoarderConfig(Fix(WithOutputPath(outPath, value)))

  def outputFormat(fmt: NJFileFormat): HoarderConfig =
    HoarderConfig(Fix(WithFileFormat(fmt, value)))

  def outputCompression(compression: NJCompression): HoarderConfig =
    HoarderConfig(Fix(WithCompression(compression, value)))

  def chunkSize(cs: ChunkSize): HoarderConfig    = HoarderConfig(Fix(WithChunkSize(cs, value)))
  def byteBuffer(bb: Information): HoarderConfig = HoarderConfig(Fix(WithByteBuffer(bb, value)))
}

private[spark] object HoarderConfig {

  def apply(outPath: NJPath): HoarderConfig =
    HoarderConfig(Fix(HoarderConfigF.InitParams[Fix[HoarderConfigF]](outPath)))
}
