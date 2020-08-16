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
  tableName: TableName,
  dbSaveMode: SaveMode,
  pathBuilder: (TableName, NJFileFormat) => String) {
  def outPath(fmt: NJFileFormat): String = pathBuilder(tableName, fmt)
}

private[database] object STParams {

  def apply(tableName: TableName): STParams =
    STParams(
      tableName = tableName,
      dbSaveMode = SaveMode.ErrorIfExists,
      pathBuilder = (tn, fmt) => s"./data/spark/database/${tn.value}/${fmt.alias}.${fmt.format}/"
    )
}
@deriveFixedPoint sealed private[database] trait STConfigF[_]

private[database] object STConfigF {
  final case class DefaultParams[K](tableName: TableName) extends STConfigF[K]

  final case class WithDbSaveMode[K](value: SaveMode, cont: K) extends STConfigF[K]

  final case class WithPathBuilder[K](value: (TableName, NJFileFormat) => String, cont: K)
      extends STConfigF[K]

  private val algebra: Algebra[STConfigF, STParams] = Algebra[STConfigF, STParams] {
    case DefaultParams(tn)     => STParams(tn)
    case WithDbSaveMode(v, c)  => STParams.dbSaveMode.set(v)(c)
    case WithPathBuilder(v, c) => STParams.pathBuilder.set(v)(c)
  }

  def evalConfig(cfg: STConfig): STParams = scheme.cata(algebra).apply(cfg.value)
}

final private[database] case class STConfig(value: Fix[STConfigF]) extends AnyVal {
  import STConfigF._

  def withDbSaveMode(sm: SaveMode): STConfig = STConfig(Fix(WithDbSaveMode(sm, value)))
  def withDBOverwrite: STConfig              = withDbSaveMode(SaveMode.Overwrite)
  def withDBAppend: STConfig                 = withDbSaveMode(SaveMode.Append)

  def withPathBuilder(f: (TableName, NJFileFormat) => String): STConfig =
    STConfig(Fix(WithPathBuilder(f, value)))

  def evalConfig: STParams = STConfigF.evalConfig(this)
}

private[database] object STConfig {

  def apply(tableName: TableName): STConfig =
    STConfig(Fix(STConfigF.DefaultParams[Fix[STConfigF]](tableName)))
}
