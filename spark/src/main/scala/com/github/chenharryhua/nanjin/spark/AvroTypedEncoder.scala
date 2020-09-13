package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StructType}

import scala.reflect.ClassTag

final class AvroTypedEncoder[A] private (val avroCodec: AvroCodec[A], te: TypedEncoder[A])
    extends Serializable {

  val classTag: ClassTag[A] = te.classTag

  val sparkSchema: StructType = TypedExpressionEncoder[A](te).schema

  private val avroStructType: StructType =
    SchemaConverters.toSqlType(avroCodec.schema).dataType match {
      case st: StructType => st
      case pt =>
        throw new Exception(s"${pt.toString} can not be convert to spark struct type")
    }

  val typedEncoder: TypedEncoder[A] =
    new TypedEncoder[A]()(classTag) {
      override val nullable: Boolean      = te.nullable
      override val jvmRepr: DataType      = te.jvmRepr
      override val catalystRepr: DataType = avroStructType

      override def fromCatalyst(path: Expression): Expression = te.fromCatalyst(path)
      override def toCatalyst(path: Expression): Expression   = te.toCatalyst(path)
    }

  val sparkEncoder: Encoder[A] = TypedExpressionEncoder(typedEncoder)

  def normalize(rdd: RDD[A])(implicit ss: SparkSession): TypedDataset[A] = {
    val schema: StructType  = TypedExpressionEncoder.targetStructType(typedEncoder)
    val encoder: Encoder[A] = TypedExpressionEncoder(typedEncoder)
    val ds: Dataset[A]      = ss.createDataset(rdd)(encoder).map(avroCodec.idConversion)(encoder)
    TypedDataset.createUnsafe[A](ss.createDataFrame(ds.toDF.rdd, schema))(typedEncoder)
  }

  def normalize(ds: Dataset[A]): TypedDataset[A] =
    normalize(ds.rdd)(ds.sparkSession)

  def normalize(tds: TypedDataset[A]): TypedDataset[A] =
    normalize(tds.dataset)

  def normalizeDF(ds: DataFrame): TypedDataset[A] =
    normalize(TypedDataset.createUnsafe(ds)(te))
}

object AvroTypedEncoder {

  def apply[A](t: TypedEncoder[A], c: AvroCodec[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](c, t)

  def apply[A](c: AvroCodec[A])(implicit t: TypedEncoder[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](c, t)
}
