package com.github.chenharryhua.nanjin.spark.table

import cats.Foldable
import cats.effect.kernel.Sync
import cats.syntax.foldable.*
import com.github.chenharryhua.nanjin.common.database.{TableName, TableQuery}
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkSessionExt}
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.zaxxer.hikari.HikariConfig
import frameless.TypedDataset
import io.circe.Decoder as JsonDecoder
import kantan.csv.{CsvConfiguration, RowDecoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import cats.syntax.functor.*

final class LoadTable[F[_], A] private[spark] (ate: AvroTypedEncoder[A], ss: SparkSession) {

  def data(ds: Dataset[A]): NJTable[F, A]       = new NJTable[F, A](ds, ate)
  def data(tds: TypedDataset[A]): NJTable[F, A] = new NJTable[F, A](tds.dataset, ate)
  def data(rdd: RDD[A]): NJTable[F, A] = new NJTable[F, A](ss.createDataset(rdd)(ate.sparkEncoder), ate)
  def data[G[_]: Foldable](list: G[A]): NJTable[F, A] =
    new NJTable[F, A](ss.createDataset(list.toList)(ate.sparkEncoder), ate)

  private def dataFolders(root: NJPath)(implicit F: Sync[F]): F[List[NJPath]] = ss.hadoop[F].dataFolders(root)

  private val emptyTable: NJTable[F, A] = NJTable.empty[F, A](ate, ss)

  def parquet(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](loaders.parquet[A](path, ss, ate), ate)

  def parquetAll(root: NJPath)(implicit F: Sync[F]): F[NJTable[F, A]] =
    dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(parquet(f)) })

  def avro(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](loaders.avro[A](path, ss, ate), ate)

  def avroAll(root: NJPath)(implicit F: Sync[F]): F[NJTable[F, A]] =
    dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(avro(f)) })

  def circe(path: NJPath)(implicit ev: JsonDecoder[A]): NJTable[F, A] =
    new NJTable[F, A](loaders.circe[A](path, ss, ate), ate)

  def circeAll(root: NJPath)(implicit F: Sync[F], ev: JsonDecoder[A]): F[NJTable[F, A]] =
    dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(circe(f)) })

  def kantan(path: NJPath, cfg: CsvConfiguration)(implicit dec: RowDecoder[A]): NJTable[F, A] =
    new NJTable[F, A](loaders.kantan[A](path, ss, ate, cfg), ate)

  def kantanAll(root: NJPath, cfg: CsvConfiguration)(implicit
    F: Sync[F],
    dec: RowDecoder[A]): F[NJTable[F, A]] =
    dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(kantan(f, cfg)) })

  def jackson(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](loaders.jackson[A](path, ss, ate), ate)

  def jacksonAll(root: NJPath)(implicit F: Sync[F]): F[NJTable[F, A]] =
    dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(jackson(f)) })

  def binAvro(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](loaders.binAvro[A](path, ss, ate), ate)

  def binAvroAll(root: NJPath)(implicit F: Sync[F]): F[NJTable[F, A]] =
    dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(binAvro(f)) })

  def objectFile(path: NJPath): NJTable[F, A] =
    new NJTable[F, A](loaders.objectFile(path, ss, ate), ate)

  def objectFileAll(root: NJPath)(implicit F: Sync[F]): F[NJTable[F, A]] =
    dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(objectFile(f)) })

  object spark {
    def json(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](loaders.spark.json[A](path, ss, ate), ate)

    def jsonAll(root: NJPath)(implicit F: Sync[F]): F[NJTable[F, A]] =
      dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(json(f)) })

    def parquet(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](loaders.spark.parquet[A](path, ss, ate), ate)

    def parquetAll(root: NJPath)(implicit F: Sync[F]): F[NJTable[F, A]] =
      dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(parquet(f)) })

    def avro(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](loaders.spark.avro[A](path, ss, ate), ate)

    def avroAll(root: NJPath)(implicit F: Sync[F]): F[NJTable[F, A]] =
      dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(avro(f)) })

    def csv(path: NJPath): NJTable[F, A] =
      new NJTable[F, A](loaders.spark.csv[A](path, ss, ate), ate)

    def csvAll(root: NJPath)(implicit F: Sync[F]): F[NJTable[F, A]] =
      dataFolders(root).map(_.foldLeft(emptyTable) { case (s, f) => s.union(csv(f)) })

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
