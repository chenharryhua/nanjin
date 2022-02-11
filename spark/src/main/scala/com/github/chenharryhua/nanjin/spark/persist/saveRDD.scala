package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.pipes.serde.NEWLINE_SEPERATOR
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
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import scalapb.GeneratedMessage

import java.io.ByteArrayOutputStream

private[spark] object saveRDD {
  private def genericRecordPair[A](rdd: RDD[A], enc: AvroEncoder[A]): RDD[(AvroKey[GenericRecord], NullWritable)] =
    rdd.mapPartitions { rcds =>
      val to = ToRecord[A](enc)
      rcds.map(rcd => (new AvroKey[GenericRecord](to.to(rcd)), NullWritable.get()))
    }

  def avro[A](rdd: RDD[A], path: NJPath, encoder: AvroEncoder[A], compression: NJCompression): Unit = {
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    compression.avro(config)
    val job = Job.getInstance(config)
    AvroJob.setOutputKeySchema(job, encoder.schema)

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
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    CompressionCodecs.setCodecConfiguration(config, CompressionCodecs.getCodecClassName(compression.name))
    config.set(NJBinaryOutputFormat.suffix, NJFileFormat.BinaryAvro.suffix)
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
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, NJFileFormat.Circe.suffix)
    CompressionCodecs.setCodecConfiguration(config, CompressionCodecs.getCodecClassName(compression.name))
    rdd
      .map(x => (NullWritable.get(), new Text(encode(x).noSpaces + NEWLINE_SEPERATOR)))
      .saveAsNewAPIHadoopFile(path.pathStr, classOf[NullWritable], classOf[Text], classOf[NJTextOutputFormat], config)
  }

  def jackson[A](rdd: RDD[A], path: NJPath, encoder: AvroEncoder[A], compression: NJCompression): Unit = {
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    CompressionCodecs.setCodecConfiguration(config, CompressionCodecs.getCodecClassName(compression.name))
    val job = Job.getInstance(config)
    AvroJob.setOutputKeySchema(job, encoder.schema)
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

    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    CompressionCodecs.setCodecConfiguration(config, CompressionCodecs.getCodecClassName(compression.name))
    config.set(NJBinaryOutputFormat.suffix, NJFileFormat.ProtoBuf.suffix)
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
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, suffix)
    CompressionCodecs.setCodecConfiguration(config, CompressionCodecs.getCodecClassName(compression.name))
    rdd
      .map(a => (NullWritable.get(), new Text(a.show + NEWLINE_SEPERATOR)))
      .saveAsNewAPIHadoopFile(path.pathStr, classOf[NullWritable], classOf[Text], classOf[NJTextOutputFormat], config)
  }

  def kantanCsv[A](
    rdd: RDD[A],
    path: NJPath,
    compression: NJCompression,
    csvConfiguration: CsvConfiguration,
    encoder: HeaderEncoder[A]): Unit = {
    val config: Configuration = new Configuration(rdd.sparkContext.hadoopConfiguration)
    config.set(NJTextOutputFormat.suffix, NJFileFormat.Kantan.suffix)
    CompressionCodecs.setCodecConfiguration(config, CompressionCodecs.getCodecClassName(compression.name))
    rdd
      .mapPartitions(iter => new KantanCsvIterator[A](encoder, csvConfiguration, iter), preservesPartitioning = true)
      .saveAsNewAPIHadoopFile(path.pathStr, classOf[NullWritable], classOf[Text], classOf[NJTextOutputFormat], config)
  }
}
