package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.terminals.NEWLINE_SEPERATOR
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.{AvroOutputStream, Encoder as AvroEncoder, ToRecord}
import io.circe.{Encoder as JsonEncoder, Json}
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import scalapb.GeneratedMessage

import java.io.ByteArrayOutputStream

private[spark] object saveRDD {
  private def genericRecordPair[A](rdd: RDD[A], enc: AvroEncoder[A]): RDD[(AvroKey[GenericRecord], NullWritable)] =
    rdd.mapPartitions { rcds =>
      val to: ToRecord[A] = ToRecord[A](enc)
      rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
    }

  def avro[A](rdd: RDD[A], path: NJPath, encoder: AvroEncoder[A], compression: NJCompression): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compression.avro(config)
    val job: Job = Job.getInstance(config)
    AvroJob.setOutputKeySchema(job, encoder.schema)
    // run
    genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile(
      path.pathStr,
      classOf[AvroKey[GenericRecord]],
      classOf[NullWritable],
      classOf[NJAvroKeyOutputFormat],
      job.getConfiguration)
  }

  def binAvro[A](rdd: RDD[A], path: NJPath, encoder: AvroEncoder[A], compression: NJCompression): Unit = {
    def bytesWritable(a: A): BytesWritable = {
      val os  = new ByteArrayOutputStream()
      val aos = AvroOutputStream.binary(encoder).to(os).build()
      aos.write(a)
      aos.flush()
      aos.close()
      new BytesWritable(os.toByteArray)
    }
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compression.set(config)
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

  def circe[A: JsonEncoder](rdd: RDD[A], path: NJPath, compression: NJCompression, isKeepNull: Boolean): Unit = {
    val encode: A => Json = a => if (isKeepNull) JsonEncoder[A].apply(a) else JsonEncoder[A].apply(a).deepDropNullValues
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, NJFileFormat.Circe.suffix)
    compression.set(config)
    // run
    rdd
      .map(x => (NullWritable.get(), new Text(encode(x).noSpaces + NEWLINE_SEPERATOR)))
      .saveAsNewAPIHadoopFile(path.pathStr, classOf[NullWritable], classOf[Text], classOf[NJTextOutputFormat], config)
  }

  def jackson[A](rdd: RDD[A], path: NJPath, encoder: AvroEncoder[A], compression: NJCompression): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compression.set(config)
    val job = Job.getInstance(config)
    AvroJob.setOutputKeySchema(job, encoder.schema)
    // run
    genericRecordPair(rdd, encoder).saveAsNewAPIHadoopFile(
      path.pathStr,
      classOf[AvroKey[GenericRecord]],
      classOf[NullWritable],
      classOf[NJJacksonKeyOutputFormat],
      job.getConfiguration)
  }

  def protobuf[A](rdd: RDD[A], path: NJPath, compression: NJCompression)(implicit enc: A <:< GeneratedMessage): Unit = {
    def bytesWritable(a: A): BytesWritable = {
      val os: ByteArrayOutputStream = new ByteArrayOutputStream()
      enc(a).writeDelimitedTo(os)
      os.close()
      new BytesWritable(os.toByteArray)
    }
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compression.set(config)
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
    compression.set(config)
    // run
    rdd
      .map(a => (NullWritable.get(), new Text(a.show + NEWLINE_SEPERATOR)))
      .saveAsNewAPIHadoopFile(path.pathStr, classOf[NullWritable], classOf[Text], classOf[NJTextOutputFormat], config)
  }

  def csv[A](
    rdd: RDD[A],
    path: NJPath,
    compression: NJCompression,
    csvCfg: CsvConfiguration,
    encoder: HeaderEncoder[A]): Unit = {
    // config
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, NJFileFormat.Kantan.suffix)
    compression.set(config)
    // run
    rdd
      .mapPartitions(iter => new KantanCsvIterator[A](encoder, csvCfg, iter), preservesPartitioning = true)
      .saveAsNewAPIHadoopFile(path.pathStr, classOf[NullWritable], classOf[Text], classOf[NJTextOutputFormat], config)
  }
}
