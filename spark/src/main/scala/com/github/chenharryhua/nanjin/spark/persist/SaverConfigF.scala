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

@Lenses final private[persist] case class SaverParams(
  singleOrMulti: SingleOrMulti,
  sparkOrRaw: SparkOrRaw,
  saveMode: SaveMode,
  parallelism: Long)

private[persist] object SaverParams {

  def apply(): SaverParams =
    SaverParams(
      SingleOrMulti.Multi,
      SparkOrRaw.Spark,
      SaveMode.Overwrite,
      defaultLocalParallelism.toLong)
}

@deriveFixedPoint sealed private[persist] trait SaverConfigF[_]

private[persist] object SaverConfigF {
  final case class DefaultParams[K]() extends SaverConfigF[K]
  final case class WithSingleOrMulti[K](value: SingleOrMulti, cont: K) extends SaverConfigF[K]
  final case class WithSparkOrHadoop[K](value: SparkOrRaw, cont: K) extends SaverConfigF[K]
  final case class WithSaveMode[K](value: SaveMode, cont: K) extends SaverConfigF[K]
  final case class WithParallelism[K](value: Long, cont: K) extends SaverConfigF[K]

  private val algebra: Algebra[SaverConfigF, SaverParams] =
    Algebra[SaverConfigF, SaverParams] {
      case DefaultParams()         => SaverParams()
      case WithSingleOrMulti(v, c) => SaverParams.singleOrMulti.set(v)(c)
      case WithSparkOrHadoop(v, c) => SaverParams.sparkOrRaw.set(v)(c)
      case WithSaveMode(v, c)      => SaverParams.saveMode.set(v)(c)
      case WithParallelism(v, c)   => SaverParams.parallelism.set(v)(c)
    }

  def evalConfig(cfg: SaverConfig): SaverParams = scheme.cata(algebra).apply(cfg.value)
}

final private[persist] case class SaverConfig(value: Fix[SaverConfigF]) {
  import SaverConfigF._
  val evalConfig: SaverParams = SaverConfigF.evalConfig(this)

  def withSingle: SaverConfig = SaverConfig(Fix(WithSingleOrMulti(SingleOrMulti.Single, value)))
  def withMulti: SaverConfig  = SaverConfig(Fix(WithSingleOrMulti(SingleOrMulti.Multi, value)))

  def withSpark: SaverConfig = SaverConfig(Fix(WithSparkOrHadoop(SparkOrRaw.Spark, value)))
  def withRaw: SaverConfig   = SaverConfig(Fix(WithSparkOrHadoop(SparkOrRaw.Raw, value)))

  def withSaveMode(saveMode: SaveMode): SaverConfig =
    SaverConfig(Fix(WithSaveMode(saveMode, value)))

  def withError: SaverConfig     = withSaveMode(SaveMode.ErrorIfExists)
  def withIgnore: SaverConfig    = withSaveMode(SaveMode.Ignore)
  def withOverwrite: SaverConfig = withSaveMode(SaveMode.Overwrite)

  def withParallel(num: Long): SaverConfig =
    SaverConfig(Fix(WithParallelism(num, value)))
}

private[persist] object SaverConfig {

  def apply(): SaverConfig = SaverConfig(Fix(SaverConfigF.DefaultParams()))
}
