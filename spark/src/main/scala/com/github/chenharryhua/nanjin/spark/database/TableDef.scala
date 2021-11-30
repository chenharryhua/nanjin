package com.github.chenharryhua.nanjin.spark.database

import com.github.chenharryhua.nanjin.common.database.TableName
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}
import frameless.TypedEncoder

final case class TableDef[A] private (tableName: TableName, ate: AvroTypedEncoder[A], unloadQuery: Option[String])

object TableDef {

  def apply[A: AvroEncoder: AvroDecoder: SchemaFor: TypedEncoder](tableName: TableName): TableDef[A] =
    new TableDef[A](tableName, AvroTypedEncoder[A], None)

  def apply[A](tableName: TableName, codec: AvroCodec[A])(implicit typedEncoder: TypedEncoder[A]) =
    new TableDef[A](tableName, AvroTypedEncoder(codec), None)

  def apply[A](tableName: TableName, codec: AvroCodec[A], unloadQuery: String)(implicit typedEncoder: TypedEncoder[A]) =
    new TableDef[A](tableName, AvroTypedEncoder(codec), Some(unloadQuery))
}
