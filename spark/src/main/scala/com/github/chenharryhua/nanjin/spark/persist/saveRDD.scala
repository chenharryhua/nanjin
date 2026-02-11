package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.terminals.{
  csvRow,
  headerWithCrlf,
  toHadoopPath,
  Compression,
  FileFormat
}
import com.sksamuel.avro4s.{AvroOutputStream, Encoder as AvroEncoder, ToRecord}
import io.circe.{Encoder as JsonEncoder, Json}
import io.lemonlabs.uri.Url
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

  def avro[A](rdd: RDD[A], path: Url, encoder: AvroEncoder[A], compression: Compression): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compressionConfig.avro(config, compression)
    val job: Job = Job.getInstance(config)
    AvroJob.setOutputKeySchema(job, encoder.schema)
    // run
    val toRecord: ToRecord[A] = ToRecord[A](encoder)
    rdd.mapPartitions { rcds =>
      rcds.map(rcd => (new AvroKey[GenericRecord](toRecord.to(rcd)), NullWritable.get()))
    }.saveAsNewAPIHadoopFile(
      toHadoopPath(path).toString,
      classOf[AvroKey[GenericRecord]],
      classOf[NullWritable],
      classOf[NJAvroKeyOutputFormat],
      job.getConfiguration)
  }

  def binAvro[A](rdd: RDD[A], path: Url, encoder: AvroEncoder[A], compression: Compression): Unit = {
    // Hadoop configuration
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compressionConfig.set(config, compression)
    config.set(NJBinaryOutputFormat.suffix, FileFormat.BinaryAvro.suffix)

    rdd.mapPartitions { iter =>
      val os = new ByteArrayOutputStream()
      val aos = AvroOutputStream.binary(encoder).to(os).build()

      new Iterator[(NullWritable, BytesWritable)] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): (NullWritable, BytesWritable) = {
          os.reset()
          aos.write(iter.next())
          aos.flush()
          (NullWritable.get(), new BytesWritable(os.toByteArray))
        }
      }
    }.saveAsNewAPIHadoopFile(
      toHadoopPath(path).toString,
      classOf[NullWritable],
      classOf[BytesWritable],
      classOf[NJBinaryOutputFormat],
      config
    )
  }

  def parquet[A](rdd: RDD[A], path: Url, encoder: AvroEncoder[A], compression: Compression): Unit = {
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    val job: Job = Job.getInstance(config)
    AvroParquetOutputFormat.setSchema(job, encoder.schema)
    ParquetOutputFormat.setCompression(job, compressionConfig.parquet(config, compression))
    // run
    rdd.mapPartitions { rcds =>
      val to: ToRecord[A] = ToRecord[A](encoder)
      rcds.map(rcd => (null, to.to(rcd)))
    }.saveAsNewAPIHadoopFile(
      toHadoopPath(path).toString,
      classOf[Void],
      classOf[GenericRecord],
      classOf[AvroParquetOutputFormat[GenericRecord]],
      job.getConfiguration)
  }

  def circe[A: JsonEncoder](rdd: RDD[A], path: Url, compression: Compression, isKeepNull: Boolean): Unit = {
    val encode: A => Json = a =>
      if (isKeepNull) JsonEncoder[A].apply(a) else JsonEncoder[A].apply(a).deepDropNullValues
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, FileFormat.Circe.suffix)
    compressionConfig.set(config, compression)
    // run
    rdd
      .map(x => (NullWritable.get(), new Text(encode(x).noSpaces.concat("\n"))))
      .saveAsNewAPIHadoopFile(
        toHadoopPath(path).toString,
        classOf[NullWritable],
        classOf[Text],
        classOf[NJTextOutputFormat],
        config)
  }

  def jackson[A](rdd: RDD[A], path: Url, encoder: AvroEncoder[A], compression: Compression): Unit = {
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
      toHadoopPath(path).toString,
      classOf[AvroKey[GenericRecord]],
      classOf[NullWritable],
      classOf[NJJacksonKeyOutputFormat],
      job.getConfiguration)
  }

  def protobuf[A](rdd: RDD[A], path: Url, compression: Compression)(implicit
    enc: A <:< GeneratedMessage): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compressionConfig.set(config, compression)
    config.set(NJBinaryOutputFormat.suffix, FileFormat.ProtoBuf.suffix)
    // run
    rdd.mapPartitions { iter =>
      val os = new ByteArrayOutputStream()

      new Iterator[(NullWritable, BytesWritable)] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): (NullWritable, BytesWritable) = {
          os.reset()
          enc(iter.next()).writeDelimitedTo(os)
          (NullWritable.get(), new BytesWritable(os.toByteArray))
        }
      }
    }.saveAsNewAPIHadoopFile(
      toHadoopPath(path).toString,
      classOf[NullWritable],
      classOf[BytesWritable],
      classOf[NJBinaryOutputFormat],
      config)
  }

  def text[A: Show](rdd: RDD[A], path: Url, compression: Compression, suffix: String): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, suffix)
    compressionConfig.set(config, compression)
    // run
    rdd
      .map(a => (NullWritable.get(), new Text(a.show.concat("\n"))))
      .saveAsNewAPIHadoopFile(
        toHadoopPath(path).toString,
        classOf[NullWritable],
        classOf[Text],
        classOf[NJTextOutputFormat],
        config)
  }

  def kantan[A](
    rdd: RDD[A],
    path: Url,
    compression: Compression,
    csvCfg: CsvConfiguration,
    encoder: RowEncoder[A]): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, FileFormat.Kantan.suffix)
    compressionConfig.set(config, compression)

    val header_crlf = headerWithCrlf(csvCfg).toList.mkString

    // run
    rdd
      .mapPartitions(
        iter =>
          Iterator(Tuple2(NullWritable.get(), new Text(header_crlf))) ++ // header
            iter.map(r => (NullWritable.get(), new Text(csvRow(csvCfg)(encoder.encode(r))))), // body
        preservesPartitioning = true
      )
      .saveAsNewAPIHadoopFile(
        toHadoopPath(path).toString,
        classOf[NullWritable],
        classOf[Text],
        classOf[NJTextOutputFormat],
        config)
  }
}
