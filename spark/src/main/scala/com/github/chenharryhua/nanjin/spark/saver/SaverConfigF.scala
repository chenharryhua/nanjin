package com.github.chenharryhua.nanjin.spark.saver

import cats.derived.auto.functor.kittensMkFunctor
import enumeratum.{Enum, EnumEntry}
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveFixedPoint
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

import scala.collection.immutable

sealed trait SingleOrMulti extends EnumEntry with Serializable

object SingleOrMulti extends Enum[SingleOrMulti] {
  override val values: immutable.IndexedSeq[SingleOrMulti] = findValues

  case object Single extends SingleOrMulti
  case object Multi extends SingleOrMulti
}

sealed trait SparkOrHadoop extends EnumEntry with Serializable

object SparkOrHadoop extends Enum[SingleOrMulti] {
  override val values: immutable.IndexedSeq[SingleOrMulti] = findValues

  case object Spark extends SparkOrHadoop
  case object Hadoop extends SparkOrHadoop
}

@Lenses final case class SaverParams(
  singleOrMulti: SingleOrMulti,
  sparkOrHadoop: SparkOrHadoop,
  saveMode: SaveMode)

object SaverParams {

  val default: SaverParams =
    SaverParams(SingleOrMulti.Multi, SparkOrHadoop.Hadoop, SaveMode.Overwrite)
}

@deriveFixedPoint sealed trait SaverConfigF[_]

object SaverConfigF {
  final case class DefaultParams[K]() extends SaverConfigF[K]
  final case class WithSingleOrMulti[K](value: SingleOrMulti, cont: K) extends SaverConfigF[K]
  final case class WithSparkOrHadoop[K](value: SparkOrHadoop, cont: K) extends SaverConfigF[K]
  final case class WithSaveMode[K](value: SaveMode, cont: K) extends SaverConfigF[K]

  private val algebra: Algebra[SaverConfigF, SaverParams] =
    Algebra[SaverConfigF, SaverParams] {
      case DefaultParams()         => SaverParams.default
      case WithSingleOrMulti(v, c) => SaverParams.singleOrMulti.set(v)(c)
      case WithSparkOrHadoop(v, c) => SaverParams.sparkOrHadoop.set(v)(c)
      case WithSaveMode(v, c)      => SaverParams.saveMode.set(v)(c)
    }

  def evalConfig(cfg: SaverConfig): SaverParams = scheme.cata(algebra).apply(cfg.value)
}

final case class SaverConfig(value: Fix[SaverConfigF]) {
  import SaverConfigF._
  val evalConfig: SaverParams = SaverConfigF.evalConfig(this)

  def withSingle: SaverConfig = SaverConfig(Fix(WithSingleOrMulti(SingleOrMulti.Single, value)))
  def withMulti: SaverConfig  = SaverConfig(Fix(WithSingleOrMulti(SingleOrMulti.Multi, value)))
  def withSpark: SaverConfig  = SaverConfig(Fix(WithSparkOrHadoop(SparkOrHadoop.Spark, value)))
  def withHadoop: SaverConfig = SaverConfig(Fix(WithSparkOrHadoop(SparkOrHadoop.Hadoop, value)))

  def withSaveMode(saveMode: SaveMode): SaverConfig =
    SaverConfig(Fix(WithSaveMode(saveMode, value)))
}

object SaverConfig {
  val default: SaverConfig = SaverConfig(Fix(SaverConfigF.DefaultParams[Fix[SaverConfigF]]()))
}
