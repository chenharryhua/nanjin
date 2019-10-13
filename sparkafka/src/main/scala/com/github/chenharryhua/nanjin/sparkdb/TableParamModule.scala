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
    override def defaultOptions: Map[String, String] = Map.empty

    override def value: String = "json"
  }

  case object Parquet extends FileFormat {
    override def defaultOptions: Map[String, String] = Map.empty

    override def value: String = "parquet"
  }

  case object Csv extends FileFormat {
    override def defaultOptions: Map[String, String] = Map("header" -> "true")

    override def value: String = "csv"
  }
}

@Lenses final case class TableParams(
  sparkOptions: Map[String, String],
  format: FileFormat,
  saveMode: SaveMode)

object TableParams {

  val default: TableParams = TableParams(
    Map.empty[String, String],
    FileFormat.Parquet,
    SaveMode.ErrorIfExists
  )
}

abstract private[sparkdb] class TableParamModule[F[_]: ContextShift: Concurrent, A] {
  self: TableDataset[F, A] =>

  final def withSaveMode(saveMode: SaveMode): TableDataset[F, A] =
    self.copy(tableParams = TableParams.saveMode.set(saveMode)(self.tableParams))

  final def withSparkOptions(options: Map[String, String]): TableDataset[F, A] =
    self.copy(tableParams = TableParams.sparkOptions.modify(_ ++ options)(self.tableParams))

  final def withSparkOption(key: String, value: String): TableDataset[F, A] =
    self.copy(tableParams = TableParams.sparkOptions.modify(_ + (key -> value))(self.tableParams))

  final def withFileFormat(format: FileFormat): TableDataset[F, A] =
    self.copy(tableParams = TableParams.format.set(format)(self.tableParams))
}
