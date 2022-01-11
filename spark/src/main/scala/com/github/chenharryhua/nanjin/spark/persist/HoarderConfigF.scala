package com.github.chenharryhua.nanjin.spark.persist

import cats.Functor
import com.github.chenharryhua.nanjin.common.{ChunkSize, NJFileFormat}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final private[persist] case class HoarderParams(
  format: NJFileFormat,
  outPath: String,
  saveMode: SaveMode,
  compression: Compression,
  chunkSize: ChunkSize)

private[persist] object HoarderParams {

  def apply(outPath: String): HoarderParams =
    HoarderParams(NJFileFormat.Unknown, outPath, SaveMode.Overwrite, Compression.Uncompressed, ChunkSize(1024))
}

sealed private[persist] trait HoarderConfigF[X]

private object HoarderConfigF {
  implicit val functorHoarderConfigF: Functor[HoarderConfigF] = cats.derived.semiauto.functor[HoarderConfigF]

  final case class InitParams[K](path: String) extends HoarderConfigF[K]
  final case class WithSaveMode[K](value: SaveMode, cont: K) extends HoarderConfigF[K]
  final case class WithOutputPath[K](value: String, cont: K) extends HoarderConfigF[K]
  final case class WithFileFormat[K](value: NJFileFormat, cont: K) extends HoarderConfigF[K]
  final case class WithCompression[K](value: Compression, cont: K) extends HoarderConfigF[K]
  final case class WithChunkSize[K](value: ChunkSize, cont: K) extends HoarderConfigF[K]

  private val algebra: Algebra[HoarderConfigF, HoarderParams] =
    Algebra[HoarderConfigF, HoarderParams] {
      case InitParams(v)         => HoarderParams(v)
      case WithSaveMode(v, c)    => HoarderParams.saveMode.set(v)(c)
      case WithOutputPath(v, c)  => HoarderParams.outPath.set(v)(c)
      case WithFileFormat(v, c)  => HoarderParams.format.set(v)(c)
      case WithCompression(v, c) => HoarderParams.compression.set(v)(c)
      case WithChunkSize(v, c)   => HoarderParams.chunkSize.set(v)(c)
    }

  def evalConfig(cfg: HoarderConfig): HoarderParams = scheme.cata(algebra).apply(cfg.value)
}

final private[persist] case class HoarderConfig(value: Fix[HoarderConfigF]) {
  import HoarderConfigF.*
  val evalConfig: HoarderParams = HoarderConfigF.evalConfig(this)

  def saveMode(sm: SaveMode): HoarderConfig =
    HoarderConfig(Fix(WithSaveMode(sm, value)))

  def errorMode: HoarderConfig     = saveMode(SaveMode.ErrorIfExists)
  def ignoreMode: HoarderConfig    = saveMode(SaveMode.Ignore)
  def overwriteMode: HoarderConfig = saveMode(SaveMode.Overwrite)
  def appendMode: HoarderConfig    = saveMode(SaveMode.Append)

  def outputPath(outPath: String): HoarderConfig =
    HoarderConfig(Fix(WithOutputPath(outPath, value)))

  def outputFormat(fmt: NJFileFormat): HoarderConfig =
    HoarderConfig(Fix(WithFileFormat(fmt, value)))

  def outputCompression(compression: Compression): HoarderConfig =
    HoarderConfig(Fix(WithCompression(compression, value)))

  def chunkSize(cs: ChunkSize): HoarderConfig =
    HoarderConfig(Fix(WithChunkSize(cs, value)))
}

private[persist] object HoarderConfig {

  def apply(outPath: String): HoarderConfig =
    HoarderConfig(Fix(HoarderConfigF.InitParams[Fix[HoarderConfigF]](outPath)))
}
