package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.{DatabaseName, DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.{TypedDataset, TypedEncoder}
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.CsvConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class TableDef[A] private (tableName: TableName, encoder: AvroTypedEncoder[A]) {

  def in[F[_]](dbSettings: DatabaseSettings)(implicit
    sparkSession: SparkSession): SparkTable[F, A] =
    new SparkTable[F, A](this, dbSettings, STConfig(dbSettings.database, tableName), sparkSession)
}

object TableDef {

  def apply[A: AvroEncoder: AvroDecoder: SchemaFor: TypedEncoder](
    tableName: TableName): TableDef[A] =
    new TableDef[A](tableName, AvroTypedEncoder(NJAvroCodec[A]))

  def apply[A](tableName: TableName, codec: NJAvroCodec[A])(implicit
    typedEncoder: TypedEncoder[A]) =
    new TableDef[A](tableName, AvroTypedEncoder(codec))
}

final class SparkTable[F[_], A](
  tableDef: TableDef[A],
  dbSettings: DatabaseSettings,
  cfg: STConfig,
  ss: SparkSession)
    extends Serializable {

  val ate: AvroTypedEncoder[A]            = tableDef.encoder
  implicit val te: TypedEncoder[A]        = ate.sparkTypedEncoder
  implicit val sparkSession: SparkSession = ss

  val params: STParams = cfg.evalConfig

  val tableName: TableName = tableDef.tableName

  def withQuery(query: String): SparkTable[F, A] =
    new SparkTable[F, A](tableDef, dbSettings, cfg.withQuery(query), ss)

  def withPathBuilder(f: (DatabaseName, TableName, NJFileFormat) => String): SparkTable[F, A] =
    new SparkTable[F, A](tableDef, dbSettings, cfg.withPathBuilder(f), ss)

  def fromDB: TableDataset[F, A] =
    new TableDataset[F, A](
      sd.unloadDS[A](dbSettings.connStr, dbSettings.driver, tableDef.tableName, params.query)
        .dataset,
      dbSettings,
      cfg)(ate)

  def tableDataset(rdd: RDD[A]): TableDataset[F, A] =
    new TableDataset[F, A](ate.fromRDD(rdd).dataset, dbSettings, cfg)(ate)

}
