package com.github.chenharryhua.nanjin.spark.database

import cats.derived.auto.functor._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.TableName
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveFixedPoint
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final private[database] case class STParams(
  dbSaveMode: SaveMode,
  fileSaveMode: SaveMode,
  pathBuilder: (TableName, NJFileFormat) => String,
  fileFormat: NJFileFormat)

private[database] object STParams {

  val default: STParams = STParams(
    dbSaveMode = SaveMode.ErrorIfExists,
    fileSaveMode = SaveMode.Overwrite,
    pathBuilder = (tn, fmt) => s"./data/spark/database/${tn.value}/${fmt.alias}.${fmt.format}/",
    fileFormat = NJFileFormat.Parquet
  )
}
@deriveFixedPoint sealed private[database] trait STConfigF[_]

private[database] object STConfigF {
  final case class DefaultParams[K]() extends STConfigF[K]

  final case class WithFileFormat[K](value: NJFileFormat, cont: K) extends STConfigF[K]

  final case class WithDbSaveMode[K](value: SaveMode, cont: K) extends STConfigF[K]
  final case class WithFileSaveMode[K](value: SaveMode, cont: K) extends STConfigF[K]

  final case class WithPathBuilder[K](value: (TableName, NJFileFormat) => String, cont: K)
      extends STConfigF[K]

  private val algebra: Algebra[STConfigF, STParams] = Algebra[STConfigF, STParams] {
    case DefaultParams()        => STParams.default
    case WithFileFormat(v, c)   => STParams.fileFormat.set(v)(c)
    case WithDbSaveMode(v, c)   => STParams.dbSaveMode.set(v)(c)
    case WithFileSaveMode(v, c) => STParams.fileSaveMode.set(v)(c)
    case WithPathBuilder(v, c)  => STParams.pathBuilder.set(v)(c)
  }

  def evalConfig(cfg: STConfig): STParams = scheme.cata(algebra).apply(cfg.value)
}

final private[database] case class STConfig(value: Fix[STConfigF]) extends AnyVal {
  import STConfigF._
  private def withFileFormat(ff: NJFileFormat): STConfig = STConfig(Fix(WithFileFormat(ff, value)))

  def withJackson: STConfig = withFileFormat(NJFileFormat.Jackson)
  def withAvro: STConfig    = withFileFormat(NJFileFormat.Avro)
  def withParquet: STConfig = withFileFormat(NJFileFormat.Parquet)

  def withDbSaveMode(sm: SaveMode): STConfig = STConfig(Fix(WithDbSaveMode(sm, value)))
  def withDBOverwrite: STConfig              = withDbSaveMode(SaveMode.Overwrite)
  def withDBAppend: STConfig                 = withDbSaveMode(SaveMode.Append)

  def withFileSaveMode(sm: SaveMode): STConfig = STConfig(Fix(WithFileSaveMode(sm, value)))
  def withFileOverwrite: STConfig              = withFileSaveMode(SaveMode.Overwrite)

  def withPathBuilder(f: (TableName, NJFileFormat) => String): STConfig =
    STConfig(Fix(WithPathBuilder(f, value)))

  def evalConfig: STParams = STConfigF.evalConfig(this)
}

private[database] object STConfig {
  val defaultConfig: STConfig = STConfig(Fix(STConfigF.DefaultParams[Fix[STConfigF]]()))
}
