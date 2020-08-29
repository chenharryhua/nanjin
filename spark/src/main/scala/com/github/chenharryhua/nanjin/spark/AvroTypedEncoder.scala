package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import frameless.{TypedDataset, TypedEncoder}
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

final class AvroTypedEncoder[A](typed: TypedEncoder[A], codec: NJAvroCodec[A])
    extends Serializable { self =>

  import typed.classTag

  val sparkDatatype: DataType = SchemaConverters.toSqlType(codec.schemaFor.schema).dataType

  val sparkStructType: StructType = sparkDatatype.asInstanceOf[StructType]

  val sparkAvroSchema: Schema = SchemaConverters.toAvroType(sparkDatatype)

  implicit val sparkTypedEncoder: TypedEncoder[A] = new TypedEncoder[A]()(typed.classTag) {
    override def nullable: Boolean                          = typed.nullable
    override def jvmRepr: DataType                          = typed.jvmRepr
    override def catalystRepr: DataType                     = sparkDatatype
    override def fromCatalyst(path: Expression): Expression = typed.fromCatalyst(path)
    override def toCatalyst(path: Expression): Expression   = typed.toCatalyst(path)
  }

  def fromRDD(rdd: RDD[A])(implicit ss: SparkSession): TypedDataset[A] =
    TypedDataset.create(rdd).deserialized.map(codec.idConversion)

  def fromDS(ds: Dataset[A]): TypedDataset[A] =
    TypedDataset.create(ds).deserialized.map(codec.idConversion)

  def fromDF(ds: DataFrame): TypedDataset[A] =
    TypedDataset.createUnsafe(ds).deserialized.map(codec.idConversion)
}

object AvroTypedEncoder {

  def apply[A](implicit t: TypedEncoder[A], c: NJAvroCodec[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](t, c)
}
