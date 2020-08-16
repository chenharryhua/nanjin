package com.github.chenharryhua.nanjin.spark.saver

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

sealed private[saver] trait SingleOrMulti extends EnumEntry with Serializable

private[saver] object SingleOrMulti extends Enum[SingleOrMulti] {
  override val values: immutable.IndexedSeq[SingleOrMulti] = findValues

  case object Single extends SingleOrMulti
  case object Multi extends SingleOrMulti
}

sealed private[saver] trait SparkOrHadoop extends EnumEntry with Serializable

private[saver] object SparkOrHadoop extends Enum[SingleOrMulti] {
  override val values: immutable.IndexedSeq[SingleOrMulti] = findValues

  case object Spark extends SparkOrHadoop
  case object Hadoop extends SparkOrHadoop
}

@Lenses final private[saver] case class SaverParams(
  outPath: String,
  fileFormat: NJFileFormat,
  singleOrMulti: SingleOrMulti,
  sparkOrHadoop: SparkOrHadoop,
  saveMode: SaveMode,
  parallelism: Long)

private[saver] object SaverParams {

  def apply(outPath: String, fmt: NJFileFormat): SaverParams =
    SaverParams(
      outPath,
      fmt,
      SingleOrMulti.Multi,
      SparkOrHadoop.Hadoop,
      SaveMode.Overwrite,
      defaultLocalParallelism.toLong)
}

@deriveFixedPoint sealed private[saver] trait SaverConfigF[_]

private[saver] object SaverConfigF {
  final case class DefaultParams[K](outPath: String, fmt: NJFileFormat) extends SaverConfigF[K]
  final case class WithSingleOrMulti[K](value: SingleOrMulti, cont: K) extends SaverConfigF[K]
  final case class WithSparkOrHadoop[K](value: SparkOrHadoop, cont: K) extends SaverConfigF[K]
  final case class WithSaveMode[K](value: SaveMode, cont: K) extends SaverConfigF[K]
  final case class WithParallism[K](value: Long, cont: K) extends SaverConfigF[K]

  private val algebra: Algebra[SaverConfigF, SaverParams] =
    Algebra[SaverConfigF, SaverParams] {
      case DefaultParams(p, f)     => SaverParams(p, f)
      case WithSingleOrMulti(v, c) => SaverParams.singleOrMulti.set(v)(c)
      case WithSparkOrHadoop(v, c) => SaverParams.sparkOrHadoop.set(v)(c)
      case WithSaveMode(v, c)      => SaverParams.saveMode.set(v)(c)
      case WithParallism(v, c)     => SaverParams.parallelism.set(v)(c)
    }

  def evalConfig(cfg: SaverConfig): SaverParams = scheme.cata(algebra).apply(cfg.value)
}

final private[saver] case class SaverConfig(value: Fix[SaverConfigF]) {
  import SaverConfigF._
  val evalConfig: SaverParams = SaverConfigF.evalConfig(this)

  def withSingle: SaverConfig = SaverConfig(Fix(WithSingleOrMulti(SingleOrMulti.Single, value)))
  def withMulti: SaverConfig  = SaverConfig(Fix(WithSingleOrMulti(SingleOrMulti.Multi, value)))
  def withSpark: SaverConfig  = SaverConfig(Fix(WithSparkOrHadoop(SparkOrHadoop.Spark, value)))
  def withHadoop: SaverConfig = SaverConfig(Fix(WithSparkOrHadoop(SparkOrHadoop.Hadoop, value)))

  def withSaveMode(saveMode: SaveMode): SaverConfig =
    SaverConfig(Fix(WithSaveMode(saveMode, value)))

  def withParallism(num: Long): SaverConfig =
    SaverConfig(Fix(WithParallism(num, value)))
}

private[saver] object SaverConfig {

  def apply(outPath: String, fmt: NJFileFormat): SaverConfig =
    SaverConfig(Fix(SaverConfigF.DefaultParams[Fix[SaverConfigF]](outPath, fmt)))
}
