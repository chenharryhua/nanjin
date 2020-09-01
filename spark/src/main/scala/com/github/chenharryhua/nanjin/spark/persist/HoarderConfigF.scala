package com.github.chenharryhua.nanjin.spark.persist

import cats.derived.auto.functor.kittensMkFunctor
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.utils.defaultLocalParallelism
import enumeratum.{Enum, EnumEntry}
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveFixedPoint
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

import scala.collection.immutable

sealed private[persist] trait SingleOrMulti extends EnumEntry with Serializable

private[persist] object SingleOrMulti extends Enum[SingleOrMulti] {
  override val values: immutable.IndexedSeq[SingleOrMulti] = findValues

  case object Single extends SingleOrMulti
  case object Multi extends SingleOrMulti
}

sealed private[persist] trait SparkOrRaw extends EnumEntry with Serializable

private[persist] object SparkOrRaw extends Enum[SingleOrMulti] {
  override val values: immutable.IndexedSeq[SingleOrMulti] = findValues

  case object Spark extends SparkOrRaw
  case object Raw extends SparkOrRaw
}

@Lenses final private[persist] case class HoarderParams(
  format: NJFileFormat,
  outPath: String,
  singleOrMulti: SingleOrMulti,
  sparkOrRaw: SparkOrRaw,
  saveMode: SaveMode,
  parallelism: Long)

private[persist] object HoarderParams {

  def apply(fmt: NJFileFormat): HoarderParams =
    HoarderParams(
      fmt,
      "",
      SingleOrMulti.Multi,
      SparkOrRaw.Spark,
      SaveMode.Overwrite,
      defaultLocalParallelism.toLong)
}

@deriveFixedPoint sealed private[persist] trait HoarderConfigF[_]

private[persist] object HoarderConfigF {
  final case class DefaultParams[K](fmt: NJFileFormat) extends HoarderConfigF[K]
  final case class WithSingleOrMulti[K](value: SingleOrMulti, cont: K) extends HoarderConfigF[K]
  final case class WithSparkOrHadoop[K](value: SparkOrRaw, cont: K) extends HoarderConfigF[K]
  final case class WithSaveMode[K](value: SaveMode, cont: K) extends HoarderConfigF[K]
  final case class WithParallelism[K](value: Long, cont: K) extends HoarderConfigF[K]
  final case class WithOutputPath[K](value: String, cont: K) extends HoarderConfigF[K]

  private val algebra: Algebra[HoarderConfigF, HoarderParams] =
    Algebra[HoarderConfigF, HoarderParams] {
      case DefaultParams(fmt)      => HoarderParams(fmt)
      case WithSingleOrMulti(v, c) => HoarderParams.singleOrMulti.set(v)(c)
      case WithSparkOrHadoop(v, c) => HoarderParams.sparkOrRaw.set(v)(c)
      case WithSaveMode(v, c)      => HoarderParams.saveMode.set(v)(c)
      case WithParallelism(v, c)   => HoarderParams.parallelism.set(v)(c)
      case WithOutputPath(v, c)    => HoarderParams.outPath.set(v)(c)
    }

  def evalConfig(cfg: HoarderConfig): HoarderParams = scheme.cata(algebra).apply(cfg.value)
}

final private[persist] case class HoarderConfig(value: Fix[HoarderConfigF]) {
  import HoarderConfigF._
  val evalConfig: HoarderParams = HoarderConfigF.evalConfig(this)

  def withSingle: HoarderConfig = HoarderConfig(Fix(WithSingleOrMulti(SingleOrMulti.Single, value)))
  def withMulti: HoarderConfig  = HoarderConfig(Fix(WithSingleOrMulti(SingleOrMulti.Multi, value)))

  def withSpark: HoarderConfig = HoarderConfig(Fix(WithSparkOrHadoop(SparkOrRaw.Spark, value)))
  def withRaw: HoarderConfig   = HoarderConfig(Fix(WithSparkOrHadoop(SparkOrRaw.Raw, value)))

  def withSaveMode(saveMode: SaveMode): HoarderConfig =
    HoarderConfig(Fix(WithSaveMode(saveMode, value)))

  def withError: HoarderConfig     = withSaveMode(SaveMode.ErrorIfExists)
  def withIgnore: HoarderConfig    = withSaveMode(SaveMode.Ignore)
  def withOverwrite: HoarderConfig = withSaveMode(SaveMode.Overwrite)

  def withParallel(num: Long): HoarderConfig =
    HoarderConfig(Fix(WithParallelism(num, value)))

  def withOutPutPath(outPath: String): HoarderConfig =
    HoarderConfig(Fix(WithOutputPath(outPath, value)))
}

private[persist] object HoarderConfig {

  def apply(fmt: NJFileFormat): HoarderConfig =
    HoarderConfig(Fix(HoarderConfigF.DefaultParams[Fix[HoarderConfigF]](fmt)))
}
