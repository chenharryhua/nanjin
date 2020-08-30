package com.github.chenharryhua.nanjin.spark.persist

import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.mapreduce.{
  NJAvroKeyOutputFormat,
  NJJacksonKeyOutputFormat
}
import com.github.chenharryhua.nanjin.spark.{utils, AvroTypedEncoder}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import frameless.cats.implicits._
import io.circe.{Encoder => JsonEncoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetOutputFormat, GenericDataSupplier}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object savers {

  object rdd {

    def circe[A](rdd: RDD[A], pathStr: String)(implicit
      encoder: JsonEncoder[A],
      codec: NJAvroCodec[A]): Unit =
      rdd.map(a => encoder(codec.idConversion(a)).noSpaces).saveAsTextFile(pathStr)

    def objectFile[A](rdd: RDD[A], outPath: String): Unit =
      rdd.saveAsObjectFile(outPath)

    def avro[A](rdd: RDD[A], pathStr: String)(implicit
      avroEncoder: AvroEncoder[A],
      ss: SparkSession): Unit = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, avroEncoder.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      utils
        .genericRecordPair(rdd, avroEncoder)
        .saveAsNewAPIHadoopFile[NJAvroKeyOutputFormat](pathStr)
    }

    def jackson[A](rdd: RDD[A], pathStr: String)(implicit
      avroEncoder: AvroEncoder[A],
      ss: SparkSession): Unit = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroJob.setOutputKeySchema(job, avroEncoder.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      utils
        .genericRecordPair(rdd, avroEncoder)
        .saveAsNewAPIHadoopFile[NJJacksonKeyOutputFormat](pathStr)
    }

    def parquet[A](rdd: RDD[A], pathStr: String)(implicit
      avroEncoder: AvroEncoder[A],
      ss: SparkSession): Unit = {
      val job = Job.getInstance(ss.sparkContext.hadoopConfiguration)
      AvroParquetOutputFormat.setAvroDataSupplier(job, classOf[GenericDataSupplier])
      AvroParquetOutputFormat.setSchema(job, avroEncoder.schema)
      ss.sparkContext.hadoopConfiguration.addResource(job.getConfiguration)
      rdd // null as java Void
        .map(a => (null, avroEncoder.encode(a).asInstanceOf[GenericRecord]))
        .saveAsNewAPIHadoopFile(
          pathStr,
          classOf[Void],
          classOf[GenericRecord],
          classOf[AvroParquetOutputFormat[GenericRecord]])
    }
  }

  object tds {

    def avro[A](ds: Dataset[A], pathStr: String)(implicit ate: AvroTypedEncoder[A]): Unit =
      ate.fromDS(ds).write.mode(SaveMode.Overwrite).format("avro").save(pathStr)

    def parquet[A](ds: Dataset[A], pathStr: String)(implicit ate: AvroTypedEncoder[A]): Unit =
      ate.fromDS(ds).write.mode(SaveMode.Overwrite).parquet(pathStr)

    def json[A](ds: Dataset[A], pathStr: String)(implicit ate: AvroTypedEncoder[A]): Unit =
      ate.fromDS(ds).write.mode(SaveMode.Overwrite).json(pathStr)

    def csv[A](ds: Dataset[A], pathStr: String)(implicit ate: AvroTypedEncoder[A]): Unit =
      ate.fromDS(ds).write.mode(SaveMode.Overwrite).csv(pathStr)

    def text[A](ds: Dataset[A], pathStr: String)(implicit ate: AvroTypedEncoder[A]): Unit =
      ate.fromDS(ds).write.mode(SaveMode.Overwrite).text(pathStr)
  }
}
