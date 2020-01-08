package com.github.chenharryhua.nanjin.spark.database

import cats.data.Reader
import com.github.chenharryhua.nanjin.spark.FileFormat
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final case class SparkTableParams(
  dbSaveMode: SaveMode,
  fileSaveMode: SaveMode,
  pathBuilder: Reader[TableName, String],
  fileFormat: FileFormat) {

  def withFileFormat(fileFormat: FileFormat): SparkTableParams =
    SparkTableParams.fileFormat.set(fileFormat)(this)

  def withDBSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.dbSaveMode.set(saveMode)(this)

  def withFileSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.fileSaveMode.set(saveMode)(this)
}

object SparkTableParams {

  val default: SparkTableParams = SparkTableParams(
    SaveMode.ErrorIfExists,
    SaveMode.Overwrite,
    Reader(tn => s"./data/database/parquet/${tn.value}"),
    FileFormat.Parquet
  )
}
