package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.ClassTag

final class AvroTypedEncoder[A] private (
  val avroCodec: NJAvroCodec[A],
  sparkEncoder: TypedEncoder[A])
    extends Serializable {

  val classTag: ClassTag[A] = sparkEncoder.classTag

  val sparkSchema: StructType = TypedExpressionEncoder[A](sparkEncoder).schema

  private val avroStructType: StructType = utils.schemaToStructType(avroCodec.schema)

  private val avroEncoder: TypedEncoder[A] =
    new TypedEncoder[A]()(classTag) {
      override val nullable: Boolean      = sparkEncoder.nullable
      override val jvmRepr: DataType      = sparkEncoder.jvmRepr
      override val catalystRepr: DataType = avroStructType

      override def fromCatalyst(path: Expression): Expression = sparkEncoder.fromCatalyst(path)
      override def toCatalyst(path: Expression): Expression   = sparkEncoder.toCatalyst(path)
    }

  def fromDF(ds: DataFrame): TypedDataset[A] =
    normalize(TypedDataset.createUnsafe(ds)(sparkEncoder))

  def normalize(rdd: RDD[A])(implicit ss: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe(utils.normalizedDF(rdd, avroCodec.avroEncoder))(avroEncoder)

  def normalize(ds: Dataset[A]): TypedDataset[A] =
    normalize(ds.rdd)(ds.sparkSession)

  def normalize(tds: TypedDataset[A]): TypedDataset[A] =
    normalize(tds.dataset.rdd)(tds.sparkSession)
}

object AvroTypedEncoder {

  def apply[A](t: TypedEncoder[A], c: NJAvroCodec[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](c, t)

  def apply[A](c: NJAvroCodec[A])(implicit t: TypedEncoder[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](c, t)
}
