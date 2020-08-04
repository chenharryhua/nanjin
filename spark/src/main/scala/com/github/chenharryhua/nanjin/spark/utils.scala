package com.github.chenharryhua.nanjin.spark

import com.sksamuel.avro4s.{ToRecord, Encoder => AvroEncoder}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DataType, StructType}

object utils {

  def genericRecordPair[A](
    rdd: RDD[A],
    enc: AvroEncoder[A]): RDD[(AvroKey[GenericRecord], NullWritable)] =
    rdd.mapPartitions { rcds =>
      val to = ToRecord[A](enc)
      rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
    }

  @SuppressWarnings(Array("AsInstanceOf"))
  def rddToDataFrame[A](rdd: RDD[A], encoder: AvroEncoder[A], ss: SparkSession): DataFrame = {
    val avroSchema: Schema     = encoder.schema
    val toGR: ToRecord[A]      = ToRecord[A](encoder)
    val dataType: DataType     = SchemaConverters.toSqlType(avroSchema).dataType
    val structType: StructType = dataType.asInstanceOf[StructType]
    val rowEnconder: ExpressionEncoder[Row] =
      RowEncoder.apply(structType).resolveAndBind()

    ss.createDataFrame(
      rdd.mapPartitions { rcds =>
        val deSer: AvroDeserializer = new AvroDeserializer(avroSchema, dataType)
        rcds.map { rcd =>
          rowEnconder.fromRow(deSer.deserialize(toGR.to(rcd)).asInstanceOf[InternalRow])
        }
      },
      structType
    )
  }
}
