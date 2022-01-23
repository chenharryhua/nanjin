package com.github.chenharryhua.nanjin.spark.database

import cats.Functor
import com.github.chenharryhua.nanjin.common.PathSegment
import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final private[database] case class STParams(
  tableName: TableName,
  query: Option[String],
  dbSaveMode: SaveMode,
  replayPathBuilder: TableName => NJPath) {
  val replayPath: NJPath = replayPathBuilder(tableName)
}

private[database] object STParams {

  def apply(tableName: TableName): STParams =
    STParams(
      tableName = tableName,
      None,
      dbSaveMode = SaveMode.ErrorIfExists,
      replayPathBuilder = tn => NJPath("data/sparkDB") / PathSegment.unsafeFrom(tn.value) / "replay"
    )
}

sealed private[database] trait STConfigF[K]

private object STConfigF {
  implicit val functorSTConfigF: Functor[STConfigF] = cats.derived.semiauto.functor[STConfigF]

  final case class InitParams[K](tableName: TableName) extends STConfigF[K]

  final case class WithDbSaveMode[K](value: SaveMode, cont: K) extends STConfigF[K]

  final case class WithReplayPathBuilder[K](value: TableName => NJPath, cont: K) extends STConfigF[K]

  final case class WithQuery[K](value: String, cont: K) extends STConfigF[K]
  final case class WithTableName[K](value: TableName, count: K) extends STConfigF[K]

  private val algebra: Algebra[STConfigF, STParams] = Algebra[STConfigF, STParams] {
    case InitParams(tn)              => STParams(tn)
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

  def replayPathBuilder(f: TableName => NJPath): STConfig =
    STConfig(Fix(WithReplayPathBuilder(f, value)))

  def unloadQuery(query: String): STConfig = STConfig(Fix(WithQuery(query, value)))

  def tableName(tableName: TableName): STConfig = STConfig(Fix(WithTableName(tableName, value)))

  def evalConfig: STParams = STConfigF.evalConfig(this)
}

private[spark] object STConfig {

  def apply(tableName: TableName): STConfig =
    STConfig(Fix(STConfigF.InitParams[Fix[STConfigF]](tableName)))
}
