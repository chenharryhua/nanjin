package com.github.chenharryhua.nanjin.spark.database

import cats.data.Reader
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import eu.timepit.refined.auto._

@Lenses final case class SparkTableParams(
  dbSaveMode: SaveMode,
  fileSaveMode: SaveMode,
  pathBuilder: Reader[TableName, String]) {

  def withDBSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.dbSaveMode.set(saveMode)(this)

  def withFileSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.fileSaveMode.set(saveMode)(this)
}

object SparkTableParams {

  val default: SparkTableParams = SparkTableParams(
    SaveMode.ErrorIfExists,
    SaveMode.Overwrite,
    Reader(tn => s"./data/database/parquet/${tn.value}")
  )
}
