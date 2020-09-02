package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.database.{DatabaseName, DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.TypedEncoder
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag

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

  implicit private val ate: AvroTypedEncoder[A]   = tableDef.encoder
  implicit private val codec: NJAvroCodec[A]      = ate.avroCodec
  implicit private val te: TypedEncoder[A]        = ate.sparkTypedEncoder
  implicit private val tag: ClassTag[A]           = ate.sparkTypedEncoder.classTag
  implicit private val sparkSession: SparkSession = ss

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

  def tableDataset(ds: Dataset[A]): TableDataset[F, A] =
    new TableDataset[F, A](ate.normalize(ds).dataset, dbSettings, cfg)(ate)

  def tableDataset(ds: RDD[A]): TableDataset[F, A] =
    new TableDataset[F, A](ate.normalize(ds).dataset, dbSettings, cfg)(ate)

  object load {

    def parquet(pathStr: String): TableDataset[F, A] =
      tableDataset(loaders.parquet(pathStr).dataset)

    def avro(pathStr: String): TableDataset[F, A] =
      tableDataset(loaders.avro(pathStr).dataset)

    def circe(pathStr: String)(implicit ev: JsonDecoder[A]): TableDataset[F, A] =
      tableDataset(loaders.circe(pathStr))

    def csv(pathStr: String)(implicit ev: RowEncoder[A]): TableDataset[F, A] =
      tableDataset(loaders.csv(pathStr).dataset)

    def csv(pathStr: String, csvConfiguration: CsvConfiguration)(implicit
      ev: RowEncoder[A]): TableDataset[F, A] =
      tableDataset(loaders.csv(pathStr, csvConfiguration).dataset)

    def json(pathStr: String): TableDataset[F, A] =
      tableDataset(loaders.json(pathStr).dataset)

    def jackson(pathStr: String): TableDataset[F, A] =
      tableDataset(loaders.raw.jackson(pathStr))

  }
}
