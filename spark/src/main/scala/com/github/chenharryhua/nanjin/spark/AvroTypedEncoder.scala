package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

final class AvroTypedEncoder[A] private (
  val avroCodec: NJAvroCodec[A],
  typedEncoder: TypedEncoder[A])
    extends Serializable {

  val sparkDatatype: DataType = SchemaConverters.toSqlType(avroCodec.schema).dataType

  val sparkStructType: StructType = sparkDatatype.asInstanceOf[StructType]

  implicit val sparkTypedEncoder: TypedEncoder[A] = new TypedEncoder[A]()(typedEncoder.classTag) {
    override val nullable: Boolean      = typedEncoder.nullable
    override val jvmRepr: DataType      = typedEncoder.jvmRepr
    override val catalystRepr: DataType = sparkDatatype

    override def fromCatalyst(path: Expression): Expression = typedEncoder.fromCatalyst(path)
    override def toCatalyst(path: Expression): Expression   = typedEncoder.toCatalyst(path)
  }

  def fromDF(ds: DataFrame): TypedDataset[A] =
    TypedDataset.createUnsafe(ds)

  def normalize(rdd: RDD[A])(implicit ss: SparkSession): TypedDataset[A] =
    fromDF(utils.toDF(rdd, avroCodec.avroEncoder))

  def normalize(ds: Dataset[A]): TypedDataset[A] =
    normalize(ds.rdd)(ds.sparkSession)

}

object AvroTypedEncoder {

  def apply[A](implicit t: TypedEncoder[A], c: NJAvroCodec[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](c, t)

  def apply[A](c: NJAvroCodec[A])(implicit t: TypedEncoder[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](c, t)
}
