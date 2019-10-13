package com.github.chenharryhua.nanjin.sparkdb

import cats.effect.{Concurrent, ContextShift}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode

@Lenses final case class TableParams(sparkOptions: Map[String, String], saveMode: SaveMode)

object TableParams {

  val default: TableParams = TableParams(
    Map("header" -> "true"),
    SaveMode.ErrorIfExists
  )
}

abstract private[sparkdb] class TableParamModule[F[_]: ContextShift: Concurrent, A] {
  self: TableDataset[F, A] =>

  def withSaveMode(saveMode: SaveMode): TableDataset[F, A] =
    self.copy(tableParams = TableParams.saveMode.set(saveMode)(self.tableParams))

  def withSparkOptions(options: Map[String, String]): TableDataset[F, A] =
    self.copy(tableParams = TableParams.sparkOptions.modify(_ ++ options)(self.tableParams))

  def withSparkOption(key: String, value: String): TableDataset[F, A] =
    self.copy(tableParams = TableParams.sparkOptions.modify(_ + (key -> value))(self.tableParams))
}
