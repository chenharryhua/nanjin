package com.github.chenharryhua.nanjin.spark.persist

import cats.derived.auto.functor._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final private[persist] case class HoarderParams(
  format: NJFileFormat,
  outPath: String,
  saveMode: SaveMode,
  compression: Compression)

private[persist] object HoarderParams {

  def apply(outPath: String): HoarderParams =
    HoarderParams(NJFileFormat.Unknown, outPath, SaveMode.Overwrite, Compression.Uncompressed)
}

sealed private[persist] trait HoarderConfigF[_]

private[persist] object HoarderConfigF {
  final case class InitParams[K](path: String) extends HoarderConfigF[K]
  final case class WithSaveMode[K](value: SaveMode, cont: K) extends HoarderConfigF[K]
  final case class WithOutputPath[K](value: String, cont: K) extends HoarderConfigF[K]
  final case class WithFileFormat[K](value: NJFileFormat, cont: K) extends HoarderConfigF[K]
  final case class WithCompression[K](value: Compression, cont: K) extends HoarderConfigF[K]

  private val algebra: Algebra[HoarderConfigF, HoarderParams] =
    Algebra[HoarderConfigF, HoarderParams] {
      case InitParams(v)         => HoarderParams(v)
      case WithSaveMode(v, c)    => HoarderParams.saveMode.set(v)(c)
      case WithOutputPath(v, c)  => HoarderParams.outPath.set(v)(c)
      case WithFileFormat(v, c)  => HoarderParams.format.set(v)(c)
      case WithCompression(v, c) => HoarderParams.compression.set(v)(c)
    }

  def evalConfig(cfg: HoarderConfig): HoarderParams = scheme.cata(algebra).apply(cfg.value)
}

final private[persist] case class HoarderConfig(value: Fix[HoarderConfigF]) {
  import HoarderConfigF._
  val evalConfig: HoarderParams = HoarderConfigF.evalConfig(this)

  def withSaveMode(saveMode: SaveMode): HoarderConfig =
    HoarderConfig(Fix(WithSaveMode(saveMode, value)))

  def withError: HoarderConfig     = withSaveMode(SaveMode.ErrorIfExists)
  def withIgnore: HoarderConfig    = withSaveMode(SaveMode.Ignore)
  def withOverwrite: HoarderConfig = withSaveMode(SaveMode.Overwrite)
  def withAppend: HoarderConfig    = withSaveMode(SaveMode.Append)

  def withOutPutPath(outPath: String): HoarderConfig =
    HoarderConfig(Fix(WithOutputPath(outPath, value)))

  def withFormat(fmt: NJFileFormat): HoarderConfig =
    HoarderConfig(Fix(WithFileFormat(fmt, value)))

  def withCompression(compression: Compression): HoarderConfig =
    HoarderConfig(Fix(WithCompression(compression, value)))
}

private[persist] object HoarderConfig {

  def apply(outPath: String): HoarderConfig =
    HoarderConfig(Fix(HoarderConfigF.InitParams[Fix[HoarderConfigF]](outPath)))
}
