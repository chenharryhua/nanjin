package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.terminals.*
import com.sksamuel.avro4s.{AvroOutputStream, Encoder as AvroEncoder, ToRecord}
import io.circe.{Encoder as JsonEncoder, Json}
import kantan.csv.{CsvConfiguration, RowEncoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.AvroParquetOutputFormat
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

import java.io.ByteArrayOutputStream

private[spark] object saveRDD {

  def avro[A](rdd: RDD[A], path: NJPath, encoder: AvroEncoder[A], compression: NJCompression): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compressionConfig.avro(config, compression)
    val job: Job = Job.getInstance(config)
    AvroJob.setOutputKeySchema(job, encoder.schema)
    // run
    rdd.mapPartitions { rcds =>
      val to: ToRecord[A] = ToRecord[A](encoder)
      rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
    }.saveAsNewAPIHadoopFile(
      path.pathStr,
      classOf[AvroKey[GenericRecord]],
      classOf[NullWritable],
      classOf[NJAvroKeyOutputFormat],
      job.getConfiguration)
  }

  def binAvro[A](rdd: RDD[A], path: NJPath, encoder: AvroEncoder[A], compression: NJCompression): Unit = {
    def bytesWritable(a: A): BytesWritable = {
      val os  = new ByteArrayOutputStream
      val aos = AvroOutputStream.binary(encoder).to(os).build()
      aos.write(a)
      aos.flush()
      aos.close()
      new BytesWritable(os.toByteArray)
    }
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compressionConfig.set(config, compression)
    config.set(NJBinaryOutputFormat.suffix, NJFileFormat.BinaryAvro.suffix)
    // run
    rdd
      .map(x => (NullWritable.get(), bytesWritable(x)))
      .saveAsNewAPIHadoopFile(
        path.pathStr,
        classOf[NullWritable],
        classOf[BytesWritable],
        classOf[NJBinaryOutputFormat],
        config)
  }

  def parquet[A](rdd: RDD[A], path: NJPath, encoder: AvroEncoder[A], compression: NJCompression): Unit = {
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val job: Job              = Job.getInstance(config)
    AvroParquetOutputFormat.setSchema(job, encoder.schema)
    ParquetOutputFormat.setCompression(job, compressionConfig.parquet(config, compression))
    // run
    rdd.mapPartitions { rcds =>
      val to: ToRecord[A] = ToRecord[A](encoder)
      rcds.map(rcd => (null, to.to(rcd)))
    }.saveAsNewAPIHadoopFile(
      path.pathStr,
      classOf[Void],
      classOf[GenericRecord],
      classOf[AvroParquetOutputFormat[GenericRecord]],
      job.getConfiguration)
  }

  def circe[A: JsonEncoder](
    rdd: RDD[A],
    path: NJPath,
    compression: NJCompression,
    isKeepNull: Boolean): Unit = {
    val encode: A => Json = a =>
      if (isKeepNull) JsonEncoder[A].apply(a) else JsonEncoder[A].apply(a).deepDropNullValues
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, NJFileFormat.Circe.suffix)
    compressionConfig.set(config, compression)
    // run
    rdd
      .map(x => (NullWritable.get(), new Text(encode(x).noSpaces.concat(System.lineSeparator()))))
      .saveAsNewAPIHadoopFile(
        path.pathStr,
        classOf[NullWritable],
        classOf[Text],
        classOf[NJTextOutputFormat],
        config)
  }

  def jackson[A](rdd: RDD[A], path: NJPath, encoder: AvroEncoder[A], compression: NJCompression): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compressionConfig.set(config, compression)
    val job = Job.getInstance(config)
    AvroJob.setOutputKeySchema(job, encoder.schema)
    // run
    rdd.mapPartitions { rcds =>
      val to: ToRecord[A] = ToRecord[A](encoder)
      rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
    }.saveAsNewAPIHadoopFile(
      path.pathStr,
      classOf[AvroKey[GenericRecord]],
      classOf[NullWritable],
      classOf[NJJacksonKeyOutputFormat],
      job.getConfiguration)
  }

  def protobuf[A](rdd: RDD[A], path: NJPath, compression: NJCompression)(implicit
    enc: A <:< GeneratedMessage): Unit = {
    def bytesWritable(a: A): BytesWritable = {
      val os: ByteArrayOutputStream = new ByteArrayOutputStream
      enc(a).writeDelimitedTo(os)
      os.close()
      new BytesWritable(os.toByteArray)
    }
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compressionConfig.set(config, compression)
    config.set(NJBinaryOutputFormat.suffix, NJFileFormat.ProtoBuf.suffix)
    // run
    rdd
      .map(x => (NullWritable.get(), bytesWritable(x)))
      .saveAsNewAPIHadoopFile(
        path.pathStr,
        classOf[NullWritable],
        classOf[BytesWritable],
        classOf[NJBinaryOutputFormat],
        config)
  }

  def text[A: Show](rdd: RDD[A], path: NJPath, compression: NJCompression, suffix: String): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, suffix)
    compressionConfig.set(config, compression)
    // run
    rdd
      .map(a => (NullWritable.get(), new Text(a.show.concat(System.lineSeparator()))))
      .saveAsNewAPIHadoopFile(
        path.pathStr,
        classOf[NullWritable],
        classOf[Text],
        classOf[NJTextOutputFormat],
        config)
  }

  def kantan[A](
    rdd: RDD[A],
    path: NJPath,
    compression: NJCompression,
    csvCfg: CsvConfiguration,
    encoder: RowEncoder[A]): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, NJFileFormat.Kantan.suffix)
    compressionConfig.set(config, compression)

    val header_crlf = csvHeader(csvCfg).toList.mkString

    // run
    rdd
      .mapPartitions(
        iter =>
          Iterator(Tuple2(NullWritable.get(), new Text(header_crlf))) ++ // header
            iter.map(r => (NullWritable.get(), new Text(csvRow(csvCfg)(encoder.encode(r))))), // body
        preservesPartitioning = true
      )
      .saveAsNewAPIHadoopFile(
        path.pathStr,
        classOf[NullWritable],
        classOf[Text],
        classOf[NJTextOutputFormat],
        config)
  }
}
