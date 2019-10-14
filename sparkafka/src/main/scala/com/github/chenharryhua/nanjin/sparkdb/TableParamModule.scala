package com.github.chenharryhua.nanjin.sparkdb

import cats.effect.{Concurrent, ContextShift}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

sealed trait FileFormat {
  def defaultOptions: Map[String, String]
  def value: String
}

object FileFormat {

  case object Json extends FileFormat {
    override val defaultOptions: Map[String, String] = Map.empty
    override val value: String                       = "json"
  }

  case object Parquet extends FileFormat {
    override val defaultOptions: Map[String, String] = Map.empty
    override val value: String                       = "parquet"
  }

  case object Csv extends FileFormat {
    override val defaultOptions: Map[String, String] = Map("header" -> "true")
    override val value: String                       = "csv"
  }
}

@Lenses final case class SparkTableParams(
  sparkOptions: Map[String, String],
  format: FileFormat,
  saveMode: SaveMode)

object SparkTableParams {

  val default: SparkTableParams = SparkTableParams(
    Map.empty[String, String],
    FileFormat.Parquet,
    SaveMode.ErrorIfExists
  )
}

abstract private[sparkdb] class TableParamModule[F[_]: ContextShift: Concurrent, A] {
  self: TableDataset[F, A] =>

  final def withSaveMode(saveMode: SaveMode): TableDataset[F, A] =
    self.copy(tableParams = SparkTableParams.saveMode.set(saveMode)(self.tableParams))

  final def withSparkOptions(options: Map[String, String]): TableDataset[F, A] =
    self.copy(tableParams = SparkTableParams.sparkOptions.modify(_ ++ options)(self.tableParams))

  final def withSparkOption(key: String, value: String): TableDataset[F, A] =
    self.copy(
      tableParams = SparkTableParams.sparkOptions.modify(_ + (key -> value))(self.tableParams))

  final def withFileFormat(format: FileFormat): TableDataset[F, A] =
    self.copy(tableParams = SparkTableParams.format.set(format)(self.tableParams))
}
