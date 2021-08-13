package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.*
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.*

import scala.reflect.ClassTag

final class AvroTypedEncoder[A] private (val avroCodec: AvroCodec[A], val typedEncoder: TypedEncoder[A])
    extends Serializable {

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
      case ex             => sys.error(s"not support yet $ex")
    }

  val classTag: ClassTag[A] = typedEncoder.classTag

  val sparkSchema: StructType  = TypedExpressionEncoder(typedEncoder).schema
  val sparkEncoder: Encoder[A] = TypedExpressionEncoder[A](typedEncoder)

  def normalize(rdd: RDD[A], ss: SparkSession): TypedDataset[A] = {
    val ds: Dataset[A] =
      ss.createDataset(rdd)(sparkEncoder).map(avroCodec.idConversion)(sparkEncoder)

    TypedDataset.createUnsafe[A](ss.createDataFrame(ds.toDF.rdd, avroStructType))(typedEncoder)
  }

  def normalize(ds: Dataset[A]): TypedDataset[A] =
    normalize(ds.rdd, ds.sparkSession)

  def normalize(tds: TypedDataset[A]): TypedDataset[A] =
    normalize(tds.dataset)

  def normalizeDF(ds: DataFrame): TypedDataset[A] =
    normalize(TypedDataset.createUnsafe(ds)(typedEncoder))

  def emptyDataset(ss: SparkSession): TypedDataset[A] =
    normalize(ss.sparkContext.emptyRDD[A](typedEncoder.classTag), ss)
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
