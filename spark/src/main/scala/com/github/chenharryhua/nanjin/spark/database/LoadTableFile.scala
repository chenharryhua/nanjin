package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.CsvConfiguration

final class LoadTableFile[F[_], A] private[database] (st: SparkDBTable[F, A]) {
  private val ate: AvroTypedEncoder[A] = st.tableDef.avroTypedEncoder

  def parquet(pathStr: String): TableDS[F, A] = {
    val tds = loaders.parquet[A](pathStr, ate, st.sparkSession)
    new TableDS[F, A](tds.dataset, st.tableDef, st.dbSettings, st.cfg)
  }

  def avro(pathStr: String): TableDS[F, A] = {
    val tds = loaders.avro[A](pathStr, ate, st.sparkSession)
    new TableDS[F, A](tds.dataset, st.tableDef, st.dbSettings, st.cfg)
  }

  def circe(pathStr: String)(implicit ev: JsonDecoder[A]): TableDS[F, A] = {
    val tds = loaders.circe[A](pathStr, ate, st.sparkSession)
    new TableDS[F, A](tds.dataset, st.tableDef, st.dbSettings, st.cfg)
  }

  def csv(pathStr: String, csvConfiguration: CsvConfiguration): TableDS[F, A] = {
    val tds = loaders.csv[A](pathStr, ate, csvConfiguration, st.sparkSession)
    new TableDS[F, A](tds.dataset, st.tableDef, st.dbSettings, st.cfg)
  }

  def csv(pathStr: String): TableDS[F, A] =
    csv(pathStr, CsvConfiguration.rfc)

  def json(pathStr: String): TableDS[F, A] = {
    val tds = loaders.json[A](pathStr, ate, st.sparkSession)
    new TableDS[F, A](tds.dataset, st.tableDef, st.dbSettings, st.cfg)
  }

  def jackson(pathStr: String): TableDS[F, A] = {
    val tds = loaders.jackson[A](pathStr, ate, st.sparkSession)
    new TableDS[F, A](tds.dataset, st.tableDef, st.dbSettings, st.cfg)
  }

  def binAvro(pathStr: String): TableDS[F, A] = {
    val tds = loaders.binAvro[A](pathStr, ate, st.sparkSession)
    new TableDS[F, A](tds.dataset, st.tableDef, st.dbSettings, st.cfg)
  }
}
