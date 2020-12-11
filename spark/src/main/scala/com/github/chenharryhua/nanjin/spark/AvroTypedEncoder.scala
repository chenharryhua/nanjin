package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder, Encoder => AvroEncoder}
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

final class AvroTypedEncoder[A] private (val avroCodec: AvroCodec[A], te: TypedEncoder[A])
    extends Serializable {

  val classTag: ClassTag[A] = te.classTag

  val originEncoder: Encoder[A] = TypedExpressionEncoder(te)
  val originSchema: StructType  = originEncoder.schema

  private val avroStructType: StructType =
    SchemaConverters.toSqlType(avroCodec.schema).dataType match {
      case st: StructType => st
      case _: StringType  => TypedExpressionEncoder[String].schema
      case _: IntegerType => TypedExpressionEncoder[Int].schema
      case _: LongType    => TypedExpressionEncoder[Long].schema
      case _: FloatType   => TypedExpressionEncoder[Float].schema
      case _: DoubleType  => TypedExpressionEncoder[Double].schema
      case _: BinaryType  => TypedExpressionEncoder[Array[Byte]].schema
      case _: DecimalType => TypedExpressionEncoder[BigDecimal].schema
      case ex             => sys.error(s"not support $ex")
    }

  val typedEncoder: TypedEncoder[A] =
    new TypedEncoder[A]()(classTag) {
      override val nullable: Boolean      = te.nullable
      override val jvmRepr: DataType      = te.jvmRepr
      override val catalystRepr: DataType = avroStructType

      override def fromCatalyst(path: Expression): Expression = te.fromCatalyst(path)
      override def toCatalyst(path: Expression): Expression   = te.toCatalyst(path)
    }

  val sparkEncoder: Encoder[A] = TypedExpressionEncoder[A](typedEncoder)

  def normalize(rdd: RDD[A])(implicit ss: SparkSession): TypedDataset[A] = {
    val ds: Dataset[A] =
      ss.createDataset(rdd)(originEncoder).map(avroCodec.idConversion)(originEncoder)

    TypedDataset.createUnsafe[A](ss.createDataFrame(ds.toDF.rdd, avroStructType))(typedEncoder)
  }

  def normalize(ds: Dataset[A]): TypedDataset[A] =
    normalize(ds.rdd)(ds.sparkSession)

  def normalize(tds: TypedDataset[A]): TypedDataset[A] =
    normalize(tds.dataset)

  def normalizeDF(ds: DataFrame): TypedDataset[A] =
    normalize(TypedDataset.createUnsafe(ds)(te))

  def emptyDataset(implicit ss: SparkSession): TypedDataset[A] =
    normalize(ss.sparkContext.emptyRDD[A](classTag))
}

object AvroTypedEncoder {

  def apply[A](te: TypedEncoder[A], ac: AvroCodec[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](ac, te)

  def apply[A](ac: AvroCodec[A])(implicit te: TypedEncoder[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](ac, te)

  def apply[A](implicit
    sf: SchemaFor[A],
    dec: AvroDecoder[A],
    enc: AvroEncoder[A],
    te: TypedEncoder[A]): AvroTypedEncoder[A] =
    new AvroTypedEncoder[A](AvroCodec[A](sf, dec, enc), te)
}
