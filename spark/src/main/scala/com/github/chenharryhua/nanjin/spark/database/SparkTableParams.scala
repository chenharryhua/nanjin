package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.NJRootPath
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import eu.timepit.refined.auto._

@Lenses final case class SparkTableParams(
  dbSaveMode: SaveMode,
  fileSaveMode: SaveMode,
  rootPath: NJRootPath) {

  def withDBSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.dbSaveMode.set(saveMode)(this)

  def withFileSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.fileSaveMode.set(saveMode)(this)
}

object SparkTableParams {

  val default: SparkTableParams = SparkTableParams(
    SaveMode.ErrorIfExists,
    SaveMode.Overwrite,
    NJRootPath("./data/database/parquet/")
  )
}
