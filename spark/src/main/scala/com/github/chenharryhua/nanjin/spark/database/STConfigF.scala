package com.github.chenharryhua.nanjin.spark.database

import cats.derived.auto.functor._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.{DatabaseName, TableName}
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveFixedPoint
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final private[database] case class STParams(
  databaseName: DatabaseName,
  tableName: TableName,
  query: Option[String],
  dbSaveMode: SaveMode,
  pathBuilder: (DatabaseName, TableName, NJFileFormat) => String) {
  def outPath(fmt: NJFileFormat): String = pathBuilder(databaseName, tableName, fmt)
}

private[database] object STParams {

  def apply(dbName: DatabaseName, tableName: TableName): STParams =
    STParams(
      databaseName = dbName,
      tableName = tableName,
      None,
      dbSaveMode = SaveMode.ErrorIfExists,
      pathBuilder = (dn, tn, fmt) =>
        s"./data/spark/database/${dn.value}/${tn.value}/${fmt.alias}.${fmt.format}/"
    )
}
@deriveFixedPoint sealed private[database] trait STConfigF[_]

private[database] object STConfigF {
  final case class DefaultParams[K](dbName: DatabaseName, tableName: TableName) extends STConfigF[K]

  final case class WithDbSaveMode[K](value: SaveMode, cont: K) extends STConfigF[K]

  final case class WithPathBuilder[K](
    value: (DatabaseName, TableName, NJFileFormat) => String,
    cont: K)
      extends STConfigF[K]

  final case class WithQuery[K](value: String, cont: K) extends STConfigF[K]
  final case class WithTableName[K](value: TableName, count: K) extends STConfigF[K]

  private val algebra: Algebra[STConfigF, STParams] = Algebra[STConfigF, STParams] {
    case DefaultParams(dn, tn) => STParams(dn, tn)
    case WithDbSaveMode(v, c)  => STParams.dbSaveMode.set(v)(c)
    case WithPathBuilder(v, c) => STParams.pathBuilder.set(v)(c)
    case WithQuery(v, c)       => STParams.query.set(Some(v))(c)
    case WithTableName(v, c)   => STParams.tableName.set(v)(c)
  }

  def evalConfig(cfg: STConfig): STParams = scheme.cata(algebra).apply(cfg.value)
}

final private[database] case class STConfig(value: Fix[STConfigF]) extends AnyVal {
  import STConfigF._

  def withDbSaveMode(sm: SaveMode): STConfig = STConfig(Fix(WithDbSaveMode(sm, value)))

  def withPathBuilder(f: (DatabaseName, TableName, NJFileFormat) => String): STConfig =
    STConfig(Fix(WithPathBuilder(f, value)))

  def withQuery(query: String): STConfig =
    STConfig(Fix(WithQuery(query, value)))

  def withTableName(tableName: TableName): STConfig =
    STConfig(Fix(WithTableName(tableName, value)))

  def evalConfig: STParams = STConfigF.evalConfig(this)
}

private[spark] object STConfig {

  def apply(dbName: DatabaseName, tableName: TableName): STConfig =
    STConfig(Fix(STConfigF.DefaultParams[Fix[STConfigF]](dbName, tableName)))
}
