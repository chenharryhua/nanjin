package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.CsvConfiguration

final class LoadTableFile[F[_], A] private[database] (st: SparkDBTable[F, A]) {
  private val ate: AvroTypedEncoder[A] = st.tableDef.avroTypedEncoder

  def parquet(pathStr: String): TableDataset[F, A] = {
    val tds = loaders.parquet[A](pathStr, ate, st.sparkSession)
    new TableDataset[F, A](tds.dataset, st.dbSettings, st.cfg, ate)
  }

  def avro(pathStr: String): TableDataset[F, A] = {
    val tds = loaders.avro[A](pathStr, ate, st.sparkSession)
    new TableDataset[F, A](tds.dataset, st.dbSettings, st.cfg, ate)
  }

  def circe(pathStr: String)(implicit ev: JsonDecoder[A]): TableDataset[F, A] = {
    val tds = loaders.circe[A](pathStr, ate, st.sparkSession)
    new TableDataset[F, A](tds.dataset, st.dbSettings, st.cfg, ate)
  }

  def csv(pathStr: String, csvConfiguration: CsvConfiguration): TableDataset[F, A] = {
    val tds = loaders.csv[A](pathStr, ate, csvConfiguration, st.sparkSession)
    new TableDataset[F, A](tds.dataset, st.dbSettings, st.cfg, ate)
  }

  def csv(pathStr: String): TableDataset[F, A] =
    csv(pathStr, CsvConfiguration.rfc)

  def json(pathStr: String): TableDataset[F, A] = {
    val tds = loaders.json[A](pathStr, ate, st.sparkSession)
    new TableDataset[F, A](tds.dataset, st.dbSettings, st.cfg, ate)
  }

  def jackson(pathStr: String): TableDataset[F, A] = {
    val tds = loaders.jackson[A](pathStr, ate, st.sparkSession)
    new TableDataset[F, A](tds.dataset, st.dbSettings, st.cfg, ate)
  }

  def binAvro(pathStr: String): TableDataset[F, A] = {
    val tds = loaders.binAvro[A](pathStr, ate, st.sparkSession)
    new TableDataset[F, A](tds.dataset, st.dbSettings, st.cfg, ate)
  }
}
