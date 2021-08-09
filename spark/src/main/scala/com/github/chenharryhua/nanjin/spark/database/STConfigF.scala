package com.github.chenharryhua.nanjin.spark.database

import cats.Functor
import com.github.chenharryhua.nanjin.common.database.{DatabaseName, TableName}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final private[database] case class STParams(
  databaseName: DatabaseName,
  tableName: TableName,
  query: Option[String],
  dbSaveMode: SaveMode,
  replayPathBuilder: (DatabaseName, TableName) => String) {
  val replayPath: String = replayPathBuilder(databaseName, tableName)
}

private[database] object STParams {

  def apply(dbName: DatabaseName, tableName: TableName): STParams =
    STParams(
      databaseName = dbName,
      tableName = tableName,
      None,
      dbSaveMode = SaveMode.ErrorIfExists,
      replayPathBuilder = (dn, tn) => s"./data/sparkDB/${dn.value}/${tn.value}/replay/".replace(":", "_")
    )
}

sealed private[database] trait STConfigF[K]

private object STConfigF {
  implicit val functorSTConfigF: Functor[STConfigF] = cats.derived.semiauto.functor[STConfigF]

  final case class InitParams[K](dbName: DatabaseName, tableName: TableName) extends STConfigF[K]

  final case class WithDbSaveMode[K](value: SaveMode, cont: K) extends STConfigF[K]

  final case class WithReplayPathBuilder[K](value: (DatabaseName, TableName) => String, cont: K) extends STConfigF[K]

  final case class WithQuery[K](value: String, cont: K) extends STConfigF[K]
  final case class WithTableName[K](value: TableName, count: K) extends STConfigF[K]

  private val algebra: Algebra[STConfigF, STParams] = Algebra[STConfigF, STParams] {
    case InitParams(dn, tn)          => STParams(dn, tn)
    case WithDbSaveMode(v, c)        => STParams.dbSaveMode.set(v)(c)
    case WithReplayPathBuilder(v, c) => STParams.replayPathBuilder.set(v)(c)
    case WithQuery(v, c)             => STParams.query.set(Some(v))(c)
    case WithTableName(v, c)         => STParams.tableName.set(v)(c)
  }

  def evalConfig(cfg: STConfig): STParams = scheme.cata(algebra).apply(cfg.value)
}

final private[database] case class STConfig(value: Fix[STConfigF]) extends AnyVal {
  import STConfigF.*

  def saveMode(sm: SaveMode): STConfig = STConfig(Fix(WithDbSaveMode(sm, value)))

  def replayPathBuilder(f: (DatabaseName, TableName) => String): STConfig =
    STConfig(Fix(WithReplayPathBuilder(f, value)))

  def unloadQuery(query: String): STConfig = STConfig(Fix(WithQuery(query, value)))

  def tableName(tableName: TableName): STConfig = STConfig(Fix(WithTableName(tableName, value)))

  def evalConfig: STParams = STConfigF.evalConfig(this)
}

private[spark] object STConfig {

  def apply(dbName: DatabaseName, tableName: TableName): STConfig =
    STConfig(Fix(STConfigF.InitParams[Fix[STConfigF]](dbName, tableName)))
}
