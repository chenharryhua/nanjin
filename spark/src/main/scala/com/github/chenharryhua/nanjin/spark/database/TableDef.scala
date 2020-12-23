package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.database.{DatabaseSettings, TableName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.TypedEncoder
import org.apache.spark.sql.SparkSession

final case class TableDef[A] private (
  tableName: TableName,
  avroTypedEncoder: AvroTypedEncoder[A],
  unloadQuery: Option[String]) {

  def in[F[_]](dbSettings: DatabaseSettings)(implicit
    sparkSession: SparkSession): SparkTable[F, A] =
    new SparkTable[F, A](this, dbSettings, STConfig(dbSettings.database, tableName), sparkSession)

}

object TableDef {

  def apply[A: AvroEncoder: AvroDecoder: SchemaFor: TypedEncoder](
    tableName: TableName): TableDef[A] =
    new TableDef[A](tableName, AvroTypedEncoder[A], None)

  def apply[A](tableName: TableName, codec: AvroCodec[A])(implicit typedEncoder: TypedEncoder[A]) =
    new TableDef[A](tableName, AvroTypedEncoder(codec), None)

  def apply[A](tableName: TableName, codec: AvroCodec[A], unloadQuery: String)(implicit
    typedEncoder: TypedEncoder[A]) =
    new TableDef[A](tableName, AvroTypedEncoder(codec), Some(unloadQuery))
}
