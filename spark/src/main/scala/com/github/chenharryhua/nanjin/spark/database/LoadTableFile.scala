package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.database.DatabaseSettings
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.CsvConfiguration
import org.apache.spark.sql.SparkSession

final class LoadTableFile[F[_], A] private[database] (
  td: TableDef[A],
  dbs: DatabaseSettings,
  cfg: STConfig,
  ss: SparkSession) {
  private val ate: AvroTypedEncoder[A] = td.avroTypedEncoder

  def parquet(pathStr: String): TableDS[F, A] = {
    val tds = loaders.parquet[A](pathStr, ate, ss)
    new TableDS[F, A](tds.dataset, td, dbs, cfg)
  }

  def avro(pathStr: String): TableDS[F, A] = {
    val tds = loaders.avro[A](pathStr, ate, ss)
    new TableDS[F, A](tds.dataset, td, dbs, cfg)
  }

  def circe(pathStr: String)(implicit ev: JsonDecoder[A]): TableDS[F, A] = {
    val tds = loaders.circe[A](pathStr, ate, ss)
    new TableDS[F, A](tds.dataset, td, dbs, cfg)
  }

  def csv(pathStr: String, csvConfiguration: CsvConfiguration): TableDS[F, A] = {
    val tds = loaders.csv[A](pathStr, ate, csvConfiguration, ss)
    new TableDS[F, A](tds.dataset, td, dbs, cfg)
  }

  def csv(pathStr: String): TableDS[F, A] =
    csv(pathStr, CsvConfiguration.rfc)

  def json(pathStr: String): TableDS[F, A] = {
    val tds = loaders.json[A](pathStr, ate, ss)
    new TableDS[F, A](tds.dataset, td, dbs, cfg)
  }

  def jackson(pathStr: String): TableDS[F, A] = {
    val tds = loaders.jackson[A](pathStr, ate, ss)
    new TableDS[F, A](tds.dataset, td, dbs, cfg)
  }

  def binAvro(pathStr: String): TableDS[F, A] = {
    val tds = loaders.binAvro[A](pathStr, ate, ss)
    new TableDS[F, A](tds.dataset, td, dbs, cfg)
  }
}
