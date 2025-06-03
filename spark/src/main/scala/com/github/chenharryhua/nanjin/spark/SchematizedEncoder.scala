package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}
import frameless.{TypedEncoder, TypedExpressionEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.*
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.*

import scala.reflect.ClassTag

final class SchematizedEncoder[A] private (val avroCodec: AvroCodec[A], val typedEncoder: TypedEncoder[A])
    extends Serializable {

  private val avroSchema: StructType =
    SchemaConverters.toSqlType(avroCodec.schema).dataType match {
      case st: StructType => st
      case primitive      => StructType(Array(StructField("value", primitive)))
    }

  val classTag: ClassTag[A] = typedEncoder.classTag

  val sparkEncoder: Encoder[A] = TypedExpressionEncoder[A](typedEncoder)
  val sparkSchema: StructType = sparkEncoder.schema

  def normalize(rdd: RDD[A], ss: SparkSession): Dataset[A] = {
    val ds: Dataset[A] = ss.createDataset(rdd.map(avroCodec.idConversion)(classTag))(sparkEncoder)
    ss.createDataFrame(ds.toDF().rdd, avroSchema).as[A](sparkEncoder)
  }

  def normalize(ds: Dataset[A]): Dataset[A] = normalize(ds.rdd, ds.sparkSession)
  def normalizeDF(df: DataFrame): Dataset[A] = normalize(df.as[A](sparkEncoder))
  def emptyDataset(ss: SparkSession): Dataset[A] = normalize(ss.emptyDataset[A](sparkEncoder))
}

object SchematizedEncoder {

  def apply[A](te: TypedEncoder[A], ac: AvroCodec[A]): SchematizedEncoder[A] =
    new SchematizedEncoder[A](ac, te)

  def apply[A](ac: AvroCodec[A])(implicit te: TypedEncoder[A]): SchematizedEncoder[A] =
    new SchematizedEncoder[A](ac, te)

  def apply[A](implicit
    sf: SchemaFor[A],
    dec: AvroDecoder[A],
    enc: AvroEncoder[A],
    te: TypedEncoder[A]): SchematizedEncoder[A] =
    new SchematizedEncoder[A](AvroCodec[A](sf, dec, enc), te)

  def apply[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): SchematizedEncoder[NJConsumerRecord[K, V]] = {
    val ote: TypedEncoder[NJConsumerRecord[K, V]] = shapeless.cachedImplicit
    SchematizedEncoder[NJConsumerRecord[K, V]](ote, NJConsumerRecord.avroCodec(keyCodec, valCodec))
  }

  def apply[K, V](topicDef: TopicDef[K, V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): SchematizedEncoder[NJConsumerRecord[K, V]] =
    apply(topicDef.rawSerdes.key.avroCodec, topicDef.rawSerdes.value.avroCodec)
}
