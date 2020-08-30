package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import frameless.{TypedDataset, TypedEncoder}
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

final class AvroTypedEncoder[A] private (
  val avroCodec: NJAvroCodec[A],
  typedEncoder: TypedEncoder[A])
    extends Serializable { self =>

  import typedEncoder.classTag

  val sparkDatatype: DataType = SchemaConverters.toSqlType(avroCodec.schemaFor.schema).dataType

  val sparkStructType: StructType = sparkDatatype.asInstanceOf[StructType]

  val sparkAvroSchema: Schema = SchemaConverters.toAvroType(sparkDatatype)

  implicit val sparkTypedEncoder: TypedEncoder[A] = new TypedEncoder[A]()(typedEncoder.classTag) {
    override def nullable: Boolean                          = typedEncoder.nullable
    override def jvmRepr: DataType                          = typedEncoder.jvmRepr
    override def catalystRepr: DataType                     = sparkDatatype
    override def fromCatalyst(path: Expression): Expression = typedEncoder.fromCatalyst(path)
    override def toCatalyst(path: Expression): Expression   = typedEncoder.toCatalyst(path)
  }

  def fromRDD(rdd: RDD[A])(implicit ss: SparkSession): TypedDataset[A] =
    TypedDataset.create(rdd).deserialized.map(avroCodec.idConversion)

  def fromDS(ds: Dataset[A]): TypedDataset[A] =
    TypedDataset.create(ds).deserialized.map(avroCodec.idConversion)

  def fromDF(ds: DataFrame): TypedDataset[A] =
    TypedDataset.createUnsafe(ds).deserialized.map(avroCodec.idConversion)
}

object AvroTypedEncoder {

  def apply[A](implicit t: TypedEncoder[A], c: NJAvroCodec[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](c, t)

  def apply[A](c: NJAvroCodec[A])(implicit t: TypedEncoder[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](c, t)
}
