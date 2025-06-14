package com.github.chenharryhua.nanjin.spark.table

import cats.Foldable
import cats.syntax.foldable.*
import com.github.chenharryhua.nanjin.common.database.{TableName, TableQuery}
import com.github.chenharryhua.nanjin.spark.SchematizedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.zaxxer.hikari.HikariConfig
import io.circe.Decoder as JsonDecoder
import io.lemonlabs.uri.Url
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

final class LoadTable[A] private[spark] (ate: SchematizedEncoder[A], ss: SparkSession) {

  def data(ds: Dataset[A]): Table[A] =
    new Table[A](ds, ate)

  def data(rdd: RDD[A]): Table[A] =
    new Table[A](ss.createDataset(rdd)(ate.sparkEncoder), ate)
  def data[G[_]: Foldable](ga: G[A]): Table[A] =
    new Table[A](ss.createDataset(ga.toList)(ate.sparkEncoder), ate)

  def empty: Table[A] = new Table[A](ate.emptyDataset(ss), ate)

  def parquet(path: Url): Table[A] =
    new Table[A](loaders.parquet[A](path, ss, ate), ate)

  def avro(path: Url): Table[A] =
    new Table[A](loaders.avro[A](path, ss, ate), ate)

  def circe(path: Url)(implicit ev: JsonDecoder[A]): Table[A] =
    new Table[A](loaders.circe[A](path, ss, ate), ate)

  def kantan(path: Url, cfg: CsvConfiguration)(implicit dec: RowDecoder[A]): Table[A] =
    new Table[A](loaders.kantan[A](path, ss, ate, cfg), ate)

  def jackson(path: Url): Table[A] =
    new Table[A](loaders.jackson[A](path, ss, ate), ate)

  def binAvro(path: Url): Table[A] =
    new Table[A](loaders.binAvro[A](path, ss, ate), ate)

  def objectFile(path: Url): Table[A] =
    new Table[A](loaders.objectFile(path, ss, ate), ate)

  object spark {
    def json(path: Url): Table[A] =
      new Table[A](loaders.spark.json[A](path, ss, ate), ate)

    def parquet(path: Url): Table[A] =
      new Table[A](loaders.spark.parquet[A](path, ss, ate), ate)

    def avro(path: Url): Table[A] =
      new Table[A](loaders.spark.avro[A](path, ss, ate), ate)

    def csv(path: Url, cfg: CsvConfiguration): Table[A] =
      new Table[A](loaders.spark.csv[A](path, ss, ate, cfg), ate)
  }

  private def toMap(hikari: HikariConfig): Map[String, String] =
    Map(
      "url" -> hikari.getJdbcUrl,
      "driver" -> hikari.getDriverClassName,
      "user" -> hikari.getUsername,
      "password" -> hikari.getPassword)

  def jdbc(hikari: HikariConfig, query: TableQuery): Table[A] = {
    val sparkOptions: Map[String, String] = toMap(hikari) + ("query" -> query.value)
    new Table[A](ate.normalizeDF(ss.read.format("jdbc").options(sparkOptions).load()), ate)
  }

  def jdbc(hikari: HikariConfig, tableName: TableName): Table[A] = {
    val sparkOptions: Map[String, String] = toMap(hikari) + ("dbtable" -> tableName.value)
    new Table[A](ate.normalizeDF(ss.read.format("jdbc").options(sparkOptions).load()), ate)
  }
}
