package com.github.chenharryhua.nanjin.spark

import org.apache.spark.sql.SaveMode

final case class StorageRootPath(value: String) extends AnyVal

final case class SparkParams(rootPath: StorageRootPath, saveMode: SaveMode)
