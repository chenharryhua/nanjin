package com.github.chenharryhua.nanjin.sparkdb

import com.github.chenharryhua.nanjin.spark.StorageRootPath
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final case class SparkTableParams(
  dbSaveMode: SaveMode,
  fileSaveMode: SaveMode,
  rootPath: StorageRootPath) {

  def withDBSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.dbSaveMode.set(saveMode)(this)

  def withFileSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.fileSaveMode.set(saveMode)(this)

  def withStorageRootPath(p: String): SparkTableParams =
    SparkTableParams.rootPath.set(StorageRootPath(p))(this)
}

object SparkTableParams {

  val default: SparkTableParams = SparkTableParams(
    SaveMode.ErrorIfExists,
    SaveMode.Overwrite,
    StorageRootPath("./data/database/parquet/")
  )
}
