package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.mapreduce.{
  NJAvroKeyOutputFormat,
  NJJacksonKeyOutputFormat
}
import com.github.chenharryhua.nanjin.spark.utils
import frameless.cats.implicits._
import io.circe.{Encoder => JsonEncoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetOutputFormat, GenericDataSupplier}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

object savers {

  def circe[A](rdd: RDD[A], pathStr: String)(implicit
    encoder: JsonEncoder[A],
    codec: NJAvroCodec[A]): Unit =
    rdd.map(a => encoder(codec.idConversion(a)).noSpaces).saveAsTextFile(pathStr)

  def json[A](rdd: RDD[A], pathStr: String)(implicit
    codec: NJAvroCodec[A],
    ss: SparkSession): Unit =
    utils.toDF(rdd, codec.avroEncoder).write.mode(SaveMode.Overwrite).json(pathStr)

  def objectFile[A](rdd: RDD[A], outPath: String): Unit =
    rdd.saveAsObjectFile(outPath)

  def csv[A](rdd: RDD[A], pathStr: String)(implicit codec: NJAvroCodec[A], ss: SparkSession): Unit =
    utils.toDF(rdd, codec.avroEncoder).write.mode(SaveMode.Overwrite).csv(pathStr)

  def text[A](rdd: RDD[A], pathStr: String)(implicit show: Show[A], codec: NJAvroCodec[A]): Unit =
    rdd.map(a => show.show(codec.idConversion(a))).saveAsTextFile(pathStr)

  def avro[A](rdd: RDD[A], pathStr: String)(implicit
    codec: NJAvroCodec[A],
    ss: SparkSession): Unit =
    utils.toDF(rdd, codec.avroEncoder).write.mode(SaveMode.Overwrite).format("avro").save(pathStr)

  def parquet[A](rdd: RDD[A], pathStr: String)(implicit
    codec: NJAvroCodec[A],
    ss: SparkSession): Unit =
    utils.toDF(rdd, codec.avroEncoder).write.mode(SaveMode.Overwrite).parquet(pathStr)

  object raw {

    def avro[A: ClassTag](rdd: RDD[A], pathStr: String)(implicit
      codec: NJAvroCodec[A],
      ss: SparkSession): Unit = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, codec.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      utils
        .genericRecordPair(rdd.map(codec.idConversion), codec.avroEncoder)
        .saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](pathStr)
    }

    def jackson[A: ClassTag](rdd: RDD[A], pathStr: String)(implicit
      codec: NJAvroCodec[A],
      ss: SparkSession): Unit = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, codec.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      utils
        .genericRecordPair(rdd.map(codec.idConversion), codec.avroEncoder)
        .saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](pathStr)
    }

    def parquet[A](rdd: RDD[A], pathStr: String)(implicit
      codec: NJAvroCodec[A],
      ss: SparkSession): Unit = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroParquetOutputFormat.setAvroDataSupplier(job, classOf[GenericDataSupplier])
      AvroParquetOutputFormat.setSchema(job, codec.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      rdd // null as java Void
        .map(a => (null, codec.avroEncoder.encode(a).asInstanceOf[GenericRecord]))
        .saveAsNewAPIHadoopFile(
          pathStr,
          classOf[Void],
          classOf[GenericRecord],
          classOf[AvroParquetOutputFormat[GenericRecord]])
    }
  }
}
