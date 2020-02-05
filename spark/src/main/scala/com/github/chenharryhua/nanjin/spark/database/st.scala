package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.{ConnectionString, DriverString, TableName}
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.sql.{SaveMode, SparkSession}

private[database] object st {

  def fromDB[A: TypedEncoder](
    connStr: ConnectionString,
    driver: DriverString,
    tableName: TableName)(
    implicit
    sparkSession: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe[A](
      sparkSession.read
        .format("jdbc")
        .option("url", connStr.value)
        .option("driver", driver.value)
        .option("dbtable", tableName.value)
        .load())

  def fromDisk[A: TypedEncoder](fileFormat: NJFileFormat, path: String)(
    implicit
    sparkSession: SparkSession): TypedDataset[A] = {
    val schema = TypedExpressionEncoder.targetStructType(TypedEncoder[A])
    TypedDataset.createUnsafe[A](
      sparkSession.read.schema(schema).format(fileFormat.format).load(path))
  }

  def save[A](
    dataset: TypedDataset[A],
    fileSaveMode: SaveMode,
    fileFormat: NJFileFormat,
    path: String): Unit =
    dataset.write.mode(fileSaveMode).format(fileFormat.format).save(path)

  def upload[A](
    dataset: TypedDataset[A],
    dbSaveMode: SaveMode,
    connStr: ConnectionString,
    driver: DriverString,
    tableName: TableName): Unit =
    dataset.write
      .mode(dbSaveMode)
      .format("jdbc")
      .option("url", connStr.value)
      .option("driver", driver.value)
      .option("dbtable", tableName.value)
      .save()
}
