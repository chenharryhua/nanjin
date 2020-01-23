package com.github.chenharryhua.nanjin.spark.database

import cats.data.Reader
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.TableName
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

final case class TablePathBuild(tableName: TableName, fileFormat: NJFileFormat)

@Lenses final case class SparkTableParams(
  dbSaveMode: SaveMode,
  fileSaveMode: SaveMode,
  pathBuilder: Reader[TablePathBuild, String],
  fileFormat: NJFileFormat) {

  def getPath(tableName: TableName): String =
    pathBuilder(TablePathBuild(tableName, fileFormat))

  def withPathBuilder(f: TablePathBuild => String): SparkTableParams =
    SparkTableParams.pathBuilder.set(Reader(f))(this)

  def withFileFormat(ff: NJFileFormat): SparkTableParams =
    SparkTableParams.fileFormat.set(ff)(this)
  def withJson: SparkTableParams    = withFileFormat(NJFileFormat.Json)
  def withAvro: SparkTableParams    = withFileFormat(NJFileFormat.Avro)
  def withParquet: SparkTableParams = withFileFormat(NJFileFormat.Parquet)

  def withDBSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.dbSaveMode.set(saveMode)(this)

  def withDBOverwrite: SparkTableParams =
    withDBSaveMode(SaveMode.Overwrite)

  def withDBAppend: SparkTableParams =
    withDBSaveMode(SaveMode.Append)

  def withFileSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.fileSaveMode.set(saveMode)(this)

  def withFileOverwrite: SparkTableParams =
    withFileSaveMode(SaveMode.Overwrite)
}

object SparkTableParams {

  val default: SparkTableParams = SparkTableParams(
    dbSaveMode   = SaveMode.ErrorIfExists,
    fileSaveMode = SaveMode.Overwrite,
    pathBuilder  = Reader(tn => s"./data/spark/database/${tn.tableName}/${tn.fileFormat}/"),
    fileFormat   = NJFileFormat.Parquet
  )
}
