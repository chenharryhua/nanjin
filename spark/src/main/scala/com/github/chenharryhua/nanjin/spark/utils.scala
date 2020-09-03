package com.github.chenharryhua.nanjin.spark

import com.sksamuel.avro4s.{ToRecord, Encoder => AvroEncoder}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object utils {

  def genericRecordPair[A](
    rdd: RDD[A],
    enc: AvroEncoder[A]): RDD[(AvroKey[GenericRecord], NullWritable)] =
    rdd.mapPartitions { rcds =>
      val to = ToRecord[A](enc)
      rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
    }

  // primitive types?
  def schemaToStructType(schema: Schema): StructType =
    SchemaConverters.toSqlType(schema).dataType match {
      case st: StructType => st
      case pt =>
        throw new Exception(s"${pt.toString} can not be convert to spark struct type")
    }

  @SuppressWarnings(Array("AsInstanceOf"))
  def normalizedDF[A](rdd: RDD[A], encoder: AvroEncoder[A])(implicit
    ss: SparkSession): DataFrame = {
    val structType: StructType = schemaToStructType(encoder.schema)

    val enRow: ExpressionEncoder.Deserializer[Row] =
      RowEncoder.apply(structType).resolveAndBind().createDeserializer()
    val rows: RDD[Row] = rdd.mapPartitions { iter =>
      val sa = new AvroDeserializer(encoder.schema, structType)
      iter.map { a =>
        enRow(sa.deserialize(encoder.encode(a)).asInstanceOf[InternalRow])
      }
    }
    ss.createDataFrame(rows, structType)
  }

  def normalizedDF[A](ds: Dataset[A], encoder: AvroEncoder[A]): DataFrame =
    normalizedDF[A](ds.rdd, encoder)(ds.sparkSession)

}
