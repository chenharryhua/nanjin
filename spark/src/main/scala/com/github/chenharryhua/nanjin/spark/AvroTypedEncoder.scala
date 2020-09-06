package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql._

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

  def normalize(rdd: RDD[A])(implicit ss: SparkSession): TypedDataset[A] =
    TypedDataset.createUnsafe(toDF(rdd))(typedEncoder)

  def normalize(ds: Dataset[A]): TypedDataset[A] =
    normalize(ds.rdd)(ds.sparkSession)

  def normalize(tds: TypedDataset[A]): TypedDataset[A] =
    normalize(tds.dataset)

  def normalizeDF(ds: DataFrame): TypedDataset[A] =
    normalize(TypedDataset.createUnsafe(ds)(te))

  @SuppressWarnings(Array("AsInstanceOf"))
  private def toDF(rdd: RDD[A])(implicit ss: SparkSession): DataFrame = {
    val enRow: ExpressionEncoder.Deserializer[Row] =
      RowEncoder.apply(avroStructType).resolveAndBind().createDeserializer()
    val rows: RDD[Row] = rdd.mapPartitions { iter =>
      val sa = new AvroDeserializer(avroCodec.schema, avroStructType)
      iter.map { a =>
        enRow(sa.deserialize(avroCodec.avroEncoder.encode(a)).asInstanceOf[InternalRow])
      }
    }
    ss.createDataFrame(rows, avroStructType)
  }
}

object AvroTypedEncoder {

  def apply[A](t: TypedEncoder[A], c: AvroCodec[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](c, t)

  def apply[A](c: AvroCodec[A])(implicit t: TypedEncoder[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](c, t)
}
