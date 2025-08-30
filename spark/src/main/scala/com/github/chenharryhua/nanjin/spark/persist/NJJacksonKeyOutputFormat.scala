package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.terminals.FileFormat
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, JsonEncoder}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroOutputFormatBase}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.{getCompressOutput, getOutputCompressorClass}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.hadoop.mapreduce.{JobContext, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.util.ReflectionUtils

import java.io.{DataOutputStream, OutputStream}

final private class NJJacksonKeyOutputFormat
    extends AvroOutputFormatBase[AvroKey[GenericRecord], NullWritable] {

  @SuppressWarnings(Array("NullParameter"))
  override def checkOutputSpecs(job: JobContext): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null) throw new InvalidJobConfException("Output directory not set.")
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job.getConfiguration)
    if (AvroJob.getOutputKeySchema(job.getConfiguration) == null)
      throw new InvalidJobConfException("schema not set")
  }

  override def getRecordWriter(
    job: TaskAttemptContext): RecordWriter[AvroKey[GenericRecord], NullWritable] = {
    val suffix: String = s"-${uuidStr(job)}.${FileFormat.Jackson.suffix}"
    val schema: Schema = AvroJob.getOutputKeySchema(job.getConfiguration)
    val conf: Configuration = job.getConfiguration
    val isCompressed: Boolean = getCompressOutput(job)
    if (isCompressed) {
      val codecClass: Class[? <: CompressionCodec] =
        getOutputCompressorClass(job, classOf[GzipCodec])
      val codec: CompressionCodec = ReflectionUtils.newInstance(codecClass, conf)
      val file: Path = getDefaultWorkFile(job, suffix + codec.getDefaultExtension)
      val fs: FileSystem = file.getFileSystem(conf)
      val fileOut: FSDataOutputStream = fs.create(file, false)
      val out: DataOutputStream = new DataOutputStream(codec.createOutputStream(fileOut))
      new JacksonKeyRecordWriter(schema, out)
    } else {
      val file: Path = getDefaultWorkFile(job, suffix)
      val fs: FileSystem = file.getFileSystem(conf)
      val out: FSDataOutputStream = fs.create(file, false)
      new JacksonKeyRecordWriter(schema, out)
    }
  }
}

final private class JacksonKeyRecordWriter(schema: Schema, os: OutputStream)
    extends RecordWriter[AvroKey[GenericRecord], NullWritable] {

  private val datumWriter: GenericDatumWriter[GenericRecord] =
    new GenericDatumWriter[GenericRecord](schema)
  private val encoder: JsonEncoder = EncoderFactory.get().jsonEncoder(schema, os)

  override def write(key: AvroKey[GenericRecord], value: NullWritable): Unit =
    datumWriter.write(key.datum(), encoder)

  override def close(context: TaskAttemptContext): Unit = {
    encoder.flush()
    os.close()
  }
}
