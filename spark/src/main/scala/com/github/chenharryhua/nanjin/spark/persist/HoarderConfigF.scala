package com.github.chenharryhua.nanjin.spark.persist

import cats.Functor
import com.github.chenharryhua.nanjin.terminals.{Compression, FileFormat}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.lemonlabs.uri.Url
import monocle.syntax.all.*
import org.apache.spark.sql.SaveMode

final private[persist] case class HoarderParams(
  format: FileFormat,
  outPath: Url,
  saveMode: SaveMode,
  compression: Compression)

private[persist] object HoarderParams {

  def apply(outPath: Url): HoarderParams =
    HoarderParams(FileFormat.Unknown, outPath, SaveMode.Overwrite, Compression.Uncompressed)
}

sealed private[persist] trait HoarderConfigF[X]

private object HoarderConfigF {
  implicit val functorHoarderConfigF: Functor[HoarderConfigF] = cats.derived.semiauto.functor[HoarderConfigF]

  final case class InitParams[K](path: Url) extends HoarderConfigF[K]
  final case class WithSaveMode[K](value: SaveMode, cont: K) extends HoarderConfigF[K]
  final case class WithFileFormat[K](value: FileFormat, cont: K) extends HoarderConfigF[K]
  final case class WithCompression[K](value: Compression, cont: K) extends HoarderConfigF[K]

  private val algebra: Algebra[HoarderConfigF, HoarderParams] =
    Algebra[HoarderConfigF, HoarderParams] {
      case InitParams(v)         => HoarderParams(v)
      case WithSaveMode(v, c)    => c.focus(_.saveMode).replace(v)
      case WithFileFormat(v, c)  => c.focus(_.format).replace(v)
      case WithCompression(v, c) => c.focus(_.compression).replace(v)
    }

  def evalConfig(cfg: HoarderConfig): HoarderParams = scheme.cata(algebra).apply(cfg.value)
}

final private[spark] case class HoarderConfig(value: Fix[HoarderConfigF]) {
  import HoarderConfigF.*
  lazy val evalConfig: HoarderParams = HoarderConfigF.evalConfig(this)

  def saveMode(sm: SaveMode): HoarderConfig =
    HoarderConfig(Fix(WithSaveMode(sm, value)))

  def outputFormat(fmt: FileFormat): HoarderConfig =
    HoarderConfig(Fix(WithFileFormat(fmt, value)))

  def outputCompression(compression: Compression): HoarderConfig =
    HoarderConfig(Fix(WithCompression(compression, value)))

}

private[spark] object HoarderConfig {

  def apply(outPath: Url): HoarderConfig =
    HoarderConfig(Fix(HoarderConfigF.InitParams[Fix[HoarderConfigF]](outPath)))
}
