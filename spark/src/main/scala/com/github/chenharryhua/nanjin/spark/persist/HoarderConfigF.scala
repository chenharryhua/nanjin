package com.github.chenharryhua.nanjin.spark.persist

import cats.Functor
import com.github.chenharryhua.nanjin.terminals.{NJCompression, NJFileFormat, NJPath}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.syntax.all.*
import org.apache.spark.sql.SaveMode

final private[persist] case class HoarderParams(
  format: NJFileFormat,
  outPath: NJPath,
  saveMode: SaveMode,
  compression: NJCompression)

private[persist] object HoarderParams {

  def apply(outPath: NJPath): HoarderParams =
    HoarderParams(NJFileFormat.Unknown, outPath, SaveMode.Overwrite, NJCompression.Uncompressed)
}

sealed private[persist] trait HoarderConfigF[X]

private object HoarderConfigF {
  implicit val functorHoarderConfigF: Functor[HoarderConfigF] = cats.derived.semiauto.functor[HoarderConfigF]

  final case class InitParams[K](path: NJPath) extends HoarderConfigF[K]
  final case class WithSaveMode[K](value: SaveMode, cont: K) extends HoarderConfigF[K]
  final case class WithFileFormat[K](value: NJFileFormat, cont: K) extends HoarderConfigF[K]
  final case class WithCompression[K](value: NJCompression, cont: K) extends HoarderConfigF[K]

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
  val evalConfig: HoarderParams = HoarderConfigF.evalConfig(this)

  def saveMode(sm: SaveMode): HoarderConfig =
    HoarderConfig(Fix(WithSaveMode(sm, value)))

  def outputFormat(fmt: NJFileFormat): HoarderConfig =
    HoarderConfig(Fix(WithFileFormat(fmt, value)))

  def outputCompression(compression: NJCompression): HoarderConfig =
    HoarderConfig(Fix(WithCompression(compression, value)))

}

private[spark] object HoarderConfig {

  def apply(outPath: NJPath): HoarderConfig =
    HoarderConfig(Fix(HoarderConfigF.InitParams[Fix[HoarderConfigF]](outPath)))
}
