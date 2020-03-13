package com.github.chenharryhua.nanjin.spark.database

import cats.data.Reader
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.TableName
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveTraverse
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

final case class TablePathBuild(tableName: TableName, fileFormat: NJFileFormat)

@Lenses final private[spark] case class STParams(
  dbSaveMode: SaveMode,
  fileSaveMode: SaveMode,
  pathBuilder: Reader[TablePathBuild, String],
  fileFormat: NJFileFormat)

private[spark] object STParams {

  val default: STParams = STParams(
    dbSaveMode   = SaveMode.ErrorIfExists,
    fileSaveMode = SaveMode.Overwrite,
    pathBuilder  = Reader(tn => s"./data/spark/database/${tn.tableName}/${tn.fileFormat}/"),
    fileFormat   = NJFileFormat.Parquet
  )
}
@deriveTraverse sealed private[spark] trait STConfigF[A]

object STConfigF {
  final case class DefaultParams[K]() extends STConfigF[K]

  final case class WithFileFormat[K](value: NJFileFormat, cont: K) extends STConfigF[K]

  final case class WithDbSaveMode[K](value: SaveMode, cont: K) extends STConfigF[K]
  final case class WithFileSaveMode[K](value: SaveMode, cont: K) extends STConfigF[K]

  final case class WithPathBuilder[K](value: Reader[TablePathBuild, String], cont: K)
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

final private[spark] case class STConfig(value: Fix[STConfigF]) extends AnyVal {
  import STConfigF._
  private def withFileFormat(ff: NJFileFormat): STConfig = STConfig(Fix(WithFileFormat(ff, value)))

  def withJson: STConfig    = withFileFormat(NJFileFormat.Json)
  def withJackson: STConfig = withFileFormat(NJFileFormat.Jackson)
  def withAvro: STConfig    = withFileFormat(NJFileFormat.Avro)
  def withParquet: STConfig = withFileFormat(NJFileFormat.Parquet)

  def withDbSaveMode(sm: SaveMode): STConfig = STConfig(Fix(WithDbSaveMode(sm, value)))
  def withDBOverwrite: STConfig              = withDbSaveMode(SaveMode.Overwrite)
  def withDBAppend: STConfig                 = withDbSaveMode(SaveMode.Append)

  def withFileSaveMode(sm: SaveMode): STConfig = STConfig(Fix(WithFileSaveMode(sm, value)))
  def withFileOverwrite: STConfig              = withFileSaveMode(SaveMode.Overwrite)

  def withPathBuilder(f: TablePathBuild => String): STConfig =
    STConfig(Fix(WithPathBuilder(Reader(f), value)))
}

private[spark] object STConfig {
  val defaultConfig: STConfig = STConfig(Fix(STConfigF.DefaultParams[Fix[STConfigF]]()))
}
