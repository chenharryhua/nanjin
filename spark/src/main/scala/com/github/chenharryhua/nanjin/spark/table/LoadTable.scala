package com.github.chenharryhua.nanjin.spark.table

import cats.Foldable
import cats.syntax.foldable.*
import com.github.chenharryhua.nanjin.common.database.{TableName, TableQuery}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.zaxxer.hikari.HikariConfig
import frameless.TypedDataset
import io.circe.Decoder as JsonDecoder
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

final class LoadTable[A] private[spark] (ate: AvroTypedEncoder[A], ss: SparkSession) {

  def data(ds: Dataset[A]): NJTable[A] =
    new NJTable[A](ds, ate)
  def data(tds: TypedDataset[A]): NJTable[A] =
    new NJTable[A](tds.dataset, ate)
  def data(rdd: RDD[A]): NJTable[A] =
    new NJTable[A](ss.createDataset(rdd)(ate.sparkEncoder), ate)
  def data[G[_]: Foldable](ga: G[A]): NJTable[A] =
    new NJTable[A](ss.createDataset(ga.toList)(ate.sparkEncoder), ate)

  def empty: NJTable[A] = new NJTable[A](ate.emptyDataset(ss), ate)

  def parquet(path: NJPath): NJTable[A] =
    new NJTable[A](loaders.parquet[A](path, ss, ate), ate)

  def avro(path: NJPath): NJTable[A] =
    new NJTable[A](loaders.avro[A](path, ss, ate), ate)

  def circe(path: NJPath)(implicit ev: JsonDecoder[A]): NJTable[A] =
    new NJTable[A](loaders.circe[A](path, ss, ate), ate)

  def kantan(path: NJPath, cfg: CsvConfiguration)(implicit dec: RowDecoder[A]): NJTable[A] =
    new NJTable[A](loaders.kantan[A](path, ss, ate, cfg), ate)

  def jackson(path: NJPath): NJTable[A] =
    new NJTable[A](loaders.jackson[A](path, ss, ate), ate)

  def binAvro(path: NJPath): NJTable[A] =
    new NJTable[A](loaders.binAvro[A](path, ss, ate), ate)

  def objectFile(path: NJPath): NJTable[A] =
    new NJTable[A](loaders.objectFile(path, ss, ate), ate)

  object spark {
    def json(path: NJPath): NJTable[A] =
      new NJTable[A](loaders.spark.json[A](path, ss, ate), ate)

    def parquet(path: NJPath): NJTable[A] =
      new NJTable[A](loaders.spark.parquet[A](path, ss, ate), ate)

    def avro(path: NJPath): NJTable[A] =
      new NJTable[A](loaders.spark.avro[A](path, ss, ate), ate)

    def csv(path: NJPath, cfg: CsvConfiguration): NJTable[A] =
      new NJTable[A](loaders.spark.csv[A](path, ss, ate, cfg), ate)

  }

  private def toMap(hikari: HikariConfig): Map[String, String] =
    Map(
      "url" -> hikari.getJdbcUrl,
      "driver" -> hikari.getDriverClassName,
      "user" -> hikari.getUsername,
      "password" -> hikari.getPassword)

  def jdbc(hikari: HikariConfig, query: TableQuery): NJTable[A] = {
    val sparkOptions: Map[String, String] = toMap(hikari) + ("query" -> query.value)
    new NJTable[A](ate.normalizeDF(ss.read.format("jdbc").options(sparkOptions).load()), ate)
  }

  def jdbc(hikari: HikariConfig, tableName: TableName): NJTable[A] = {
    val sparkOptions: Map[String, String] = toMap(hikari) + ("dbtable" -> tableName.value)
    new NJTable[A](ate.normalizeDF(ss.read.format("jdbc").options(sparkOptions).load()), ate)
  }
}
