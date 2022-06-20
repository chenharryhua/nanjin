package com.github.chenharryhua.nanjin.spark.table

import cats.Foldable
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
import cats.syntax.foldable.*

final class LoadTable[F[_], A] private[spark] (ate: AvroTypedEncoder[A], ss: SparkSession) {

  def data(ds: Dataset[A]): NJTable[F, A]       = new NJTable[F, A](ds, ate)
  def data(tds: TypedDataset[A]): NJTable[F, A] = new NJTable[F, A](tds.dataset, ate)
  def data(rdd: RDD[A]): NJTable[F, A] = new NJTable[F, A](ss.createDataset(rdd)(ate.sparkEncoder), ate)
  def data[G[_]: Foldable](list: G[A]): NJTable[F, A] =
    new NJTable[F, A](ss.createDataset(list.toList)(ate.sparkEncoder), ate)

  def parquet(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](loaders.parquet[A](path, ate, ss), ate)

  def avro(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](loaders.avro[A](path, ate, ss), ate)

  def circe(path: NJPath)(implicit ev: JsonDecoder[A]): NJTable[F, A] =
    new NJTable[F, A](loaders.circe[A](path, ate, ss), ate)

  def kantan(path: NJPath, csvConfiguration: CsvConfiguration)(implicit dec: RowDecoder[A]): NJTable[F, A] =
    new NJTable[F, A](loaders.kantan[A](path, ate, csvConfiguration, ss), ate)

  def kantan(path: NJPath)(implicit dec: RowDecoder[A]): NJTable[F, A] =
    kantan(path, CsvConfiguration.rfc)

  def jackson(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](loaders.jackson[A](path, ate, ss), ate)

  def binAvro(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](loaders.binAvro[A](path, ate, ss), ate)

  def objectFile(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](loaders.objectFile(path, ate, ss), ate)

  object spark {
    def json(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](loaders.spark.json[A](path, ate, ss), ate)

    def parquet(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](loaders.spark.parquet[A](path, ate, ss), ate)

    def avro(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](loaders.spark.avro[A](path, ate, ss), ate)

    def csv(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](loaders.spark.csv[A](path, ate, ss), ate)
  }

  private def toMap(hikari: HikariConfig): Map[String, String] =
    Map(
      "url" -> hikari.getJdbcUrl,
      "driver" -> hikari.getDriverClassName,
      "user" -> hikari.getUsername,
      "password" -> hikari.getPassword)

  def jdbc(hikari: HikariConfig, query: TableQuery): NJTable[F, A] = {
    val sparkOptions: Map[String, String] = toMap(hikari) + ("query" -> query.value)
    new NJTable[F, A](ate.normalizeDF(ss.read.format("jdbc").options(sparkOptions).load()), ate)
  }

  def jdbc(hikari: HikariConfig, tableName: TableName): NJTable[F, A] = {
    val sparkOptions: Map[String, String] = toMap(hikari) + ("dbtable" -> tableName.value)
    new NJTable[F, A](ate.normalizeDF(ss.read.format("jdbc").options(sparkOptions).load()), ate)
  }
}
