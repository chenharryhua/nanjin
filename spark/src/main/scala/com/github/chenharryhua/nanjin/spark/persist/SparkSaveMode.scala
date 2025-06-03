package com.github.chenharryhua.nanjin.spark.persist

import org.apache.spark.sql.SaveMode

object SparkSaveMode {
  val Append: SaveMode = SaveMode.Append
  val Overwrite: SaveMode = SaveMode.Overwrite
  val ErrorIfExists: SaveMode = SaveMode.ErrorIfExists
  val Ignore: SaveMode = SaveMode.Ignore
}
