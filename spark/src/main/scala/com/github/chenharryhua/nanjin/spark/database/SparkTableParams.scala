package com.github.chenharryhua.nanjin.spark.database

import cats.data.Reader
import com.github.chenharryhua.nanjin.common.NJFileFormat
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final case class SparkTableParams(
  dbSaveMode: SaveMode,
  fileSaveMode: SaveMode,
  pathBuilder: Reader[TableName, String],
  fileFormat: NJFileFormat) {

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
    SaveMode.ErrorIfExists,
    SaveMode.Overwrite,
    Reader(tn => s"./data/spark/database/${tn.value}"),
    NJFileFormat.Parquet
  )
}
