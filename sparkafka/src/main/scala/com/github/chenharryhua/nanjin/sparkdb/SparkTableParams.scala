package com.github.chenharryhua.nanjin.sparkdb

import com.github.chenharryhua.nanjin.spark.StorageRootPath
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final case class SparkTableParams(saveMode: SaveMode, rootPath: StorageRootPath) {

  def withSaveMode(saveMode: SaveMode): SparkTableParams =
    SparkTableParams.saveMode.set(saveMode)(this)

  def withStorageRootPath(p: String): SparkTableParams =
    SparkTableParams.rootPath.set(StorageRootPath(p))(this)
}

object SparkTableParams {

  val default: SparkTableParams = SparkTableParams(
    SaveMode.ErrorIfExists,
    StorageRootPath("./data/database/parquet/")
  )
}
