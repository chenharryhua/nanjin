package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.{TypedDataset, TypedEncoder}
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final case class TableDef[A] private (tableName: TableName, avroTypedEncoder: AvroTypedEncoder[A]) {

  def in[F[_]](dbSettings: DatabaseSettings)(implicit
    sparkSession: SparkSession): SparkTable[F, A] =
    new SparkTable[F, A](this, dbSettings, STConfig(dbSettings.database, tableName), sparkSession)

  object load {
    private val ate: AvroTypedEncoder[A]  = avroTypedEncoder
    implicit private val tag: ClassTag[A] = avroTypedEncoder.classTag

    def parquet(pathStr: String)(implicit ss: SparkSession): TypedDataset[A] =
      loaders.parquet[A](pathStr, ate)

    def avro(pathStr: String)(implicit ss: SparkSession): TypedDataset[A] =
      loaders.avro[A](pathStr, ate)

    def circe(pathStr: String)(implicit ev: JsonDecoder[A], ss: SparkSession): TypedDataset[A] =
      loaders.circe[A](pathStr, ate)

    def csv(pathStr: String)(implicit ev: RowEncoder[A], ss: SparkSession): TypedDataset[A] =
      loaders.csv[A](pathStr, ate)

    def csv(pathStr: String, csvConfiguration: CsvConfiguration)(implicit
      ev: RowEncoder[A],
      ss: SparkSession): TypedDataset[A] =
      loaders.csv[A](pathStr, ate, csvConfiguration)

    def json(pathStr: String)(implicit ss: SparkSession): TypedDataset[A] =
      loaders.json[A](pathStr, ate)

    def jackson(pathStr: String)(implicit ss: SparkSession): TypedDataset[A] =
      loaders.jackson[A](pathStr, ate)

    def binAvro(pathStr: String)(implicit ss: SparkSession): TypedDataset[A] =
      loaders.binAvro[A](pathStr, ate)
  }
}

object TableDef {

  def apply[A: AvroEncoder: AvroDecoder: SchemaFor: TypedEncoder](
    tableName: TableName): TableDef[A] =
    new TableDef[A](tableName, AvroTypedEncoder(AvroCodec[A]))

  def apply[A](tableName: TableName, codec: AvroCodec[A])(implicit typedEncoder: TypedEncoder[A]) =
    new TableDef[A](tableName, AvroTypedEncoder(codec))
}
