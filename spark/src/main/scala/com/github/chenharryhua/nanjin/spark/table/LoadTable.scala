package com.github.chenharryhua.nanjin.spark.table

import cats.Foldable
import cats.effect.kernel.Sync
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

final class LoadTable[F[_], A] private[spark] (ate: AvroTypedEncoder[A], ss: SparkSession)(implicit
  F: Sync[F]) {

  def data(ds: Dataset[A]): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(ds), ate)
  def data(tds: TypedDataset[A]): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(tds.dataset), ate)
  def data(rdd: RDD[A]): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(ss.createDataset(rdd)(ate.sparkEncoder)), ate)
  def data[G[_]: Foldable](ga: G[A]): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(ss.createDataset(ga.toList)(ate.sparkEncoder)), ate)

  def empty: NJTable[F, A] = new NJTable[F, A](F.interruptible(ate.emptyDataset(ss)), ate)

  def parquet(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(loaders.parquet[A](path, ss, ate)), ate)

  def avro(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(loaders.avro[A](path, ss, ate)), ate)

  def circe(path: NJPath)(implicit ev: JsonDecoder[A]): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(loaders.circe[A](path, ss, ate)), ate)

  def kantan(path: NJPath, cfg: CsvConfiguration)(implicit dec: RowDecoder[A]): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(loaders.kantan[A](path, ss, ate, cfg)), ate)

  def jackson(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(loaders.jackson[A](path, ss, ate)), ate)

  def binAvro(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(loaders.binAvro[A](path, ss, ate)), ate)

  def objectFile(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](F.interruptible(loaders.objectFile(path, ss, ate)), ate)

  object spark {
    def json(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](F.interruptible(loaders.spark.json[A](path, ss, ate)), ate)

    def parquet(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](F.interruptible(loaders.spark.parquet[A](path, ss, ate)), ate)

    def avro(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](F.interruptible(loaders.spark.avro[A](path, ss, ate)), ate)

    def csv(path: NJPath, cfg: CsvConfiguration): NJTable[F, A] =
      new NJTable[F, A](F.interruptible(loaders.spark.csv[A](path, ss, ate, cfg)), ate)

  }

  private def toMap(hikari: HikariConfig): Map[String, String] =
    Map(
      "url" -> hikari.getJdbcUrl,
      "driver" -> hikari.getDriverClassName,
      "user" -> hikari.getUsername,
      "password" -> hikari.getPassword)

  def jdbc(hikari: HikariConfig, query: TableQuery): NJTable[F, A] = {
    val sparkOptions: Map[String, String] = toMap(hikari) + ("query" -> query.value)
    new NJTable[F, A](
      F.interruptible(ate.normalizeDF(ss.read.format("jdbc").options(sparkOptions).load())),
      ate)
  }

  def jdbc(hikari: HikariConfig, tableName: TableName): NJTable[F, A] = {
    val sparkOptions: Map[String, String] = toMap(hikari) + ("dbtable" -> tableName.value)
    new NJTable[F, A](
      F.interruptible(ate.normalizeDF(ss.read.format("jdbc").options(sparkOptions).load())),
      ate)
  }
}
