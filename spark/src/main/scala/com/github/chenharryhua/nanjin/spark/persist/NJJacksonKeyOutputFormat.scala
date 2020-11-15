package com.github.chenharryhua.nanjin.spark.persist

import java.io.{DataOutputStream, OutputStream}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, JsonEncoder}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroOutputFormatBase}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.{
  getCompressOutput,
  getOutputCompressorClass
}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.util.ReflectionUtils

final class NJJacksonKeyOutputFormat
    extends AvroOutputFormatBase[AvroKey[GenericRecord], NullWritable] {

  override def getRecordWriter(
    job: TaskAttemptContext): RecordWriter[AvroKey[GenericRecord], NullWritable] = {
    val suffix         = ".json"
    val schema: Schema = AvroJob.getOutputKeySchema(job.getConfiguration)
    val conf           = job.getConfiguration
    val isCompressed   = getCompressOutput(job)
    if (isCompressed) {
      val codecClass = getOutputCompressorClass(job, classOf[GzipCodec])
      val codec      = ReflectionUtils.newInstance(codecClass, conf)
      val ext        = suffix + codec.getDefaultExtension
      val file       = getDefaultWorkFile(job, ext)
      val fs         = file.getFileSystem(conf)
      val fileOut    = fs.create(file, false)
      val out        = new DataOutputStream(codec.createOutputStream(fileOut))
      new JacksonKeyRecordWriter(schema, out)
    } else {
      val file = getDefaultWorkFile(job, suffix)
      val fs   = file.getFileSystem(conf)
      val out  = fs.create(file, false)
      new JacksonKeyRecordWriter(schema, out)
    }
  }
}

final class JacksonKeyRecordWriter(schema: Schema, os: OutputStream)
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
