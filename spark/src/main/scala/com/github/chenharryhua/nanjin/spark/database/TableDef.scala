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

final case class TableDef[A] private (tableName: TableName, encoder: AvroTypedEncoder[A]) {

  def in[F[_]](dbSettings: DatabaseSettings)(implicit
    sparkSession: SparkSession): SparkTable[F, A] =
    new SparkTable[F, A](this, dbSettings, STConfig(dbSettings.database, tableName), sparkSession)

  object load {
    implicit private val ate: AvroTypedEncoder[A] = encoder
    implicit private val codec: AvroCodec[A]      = encoder.avroCodec
    implicit private val enc: TypedEncoder[A]     = encoder.typedEncoder
    implicit private val tag: ClassTag[A]         = encoder.classTag

    def parquet(pathStr: String)(implicit ss: SparkSession): TypedDataset[A] =
      loaders.parquet(pathStr)

    def avro(pathStr: String)(implicit ss: SparkSession): TypedDataset[A] =
      loaders.avro(pathStr)

    def circe(pathStr: String)(implicit ev: JsonDecoder[A], ss: SparkSession): TypedDataset[A] =
      TypedDataset.create(loaders.circe[A](pathStr))

    def csv(pathStr: String)(implicit ev: RowEncoder[A], ss: SparkSession): TypedDataset[A] =
      loaders.csv[A](pathStr)

    def csv(pathStr: String, csvConfiguration: CsvConfiguration)(implicit
      ev: RowEncoder[A],
      ss: SparkSession): TypedDataset[A] =
      loaders.csv[A](pathStr, csvConfiguration)

    def json(pathStr: String)(implicit ss: SparkSession): TypedDataset[A] =
      loaders.json[A](pathStr)

    def jackson(pathStr: String)(implicit ss: SparkSession): TypedDataset[A] =
      TypedDataset.create(loaders.raw.jackson[A](pathStr))

  }
}

object TableDef {

  def apply[A: AvroEncoder: AvroDecoder: SchemaFor: TypedEncoder](
    tableName: TableName): TableDef[A] =
    new TableDef[A](tableName, AvroTypedEncoder(AvroCodec[A]))

  def apply[A](tableName: TableName, codec: AvroCodec[A])(implicit typedEncoder: TypedEncoder[A]) =
    new TableDef[A](tableName, AvroTypedEncoder(codec))
}
