package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.common.NJFileFormat
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, JsonEncoder}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroOutputFormatBase}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.{getCompressOutput, getOutputCompressorClass}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.util.ReflectionUtils

import java.io.{DataOutputStream, OutputStream}

final class NJJacksonKeyOutputFormat extends AvroOutputFormatBase[AvroKey[GenericRecord], NullWritable] {

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[AvroKey[GenericRecord], NullWritable] = {
    val suffix: String        = s"-${utils.uuidStr(job)}.${NJFileFormat.Jackson.suffix}"
    val schema: Schema        = AvroJob.getOutputKeySchema(job.getConfiguration)
    val conf: Configuration   = job.getConfiguration
    val isCompressed: Boolean = getCompressOutput(job)
    if (isCompressed) {
      val codecClass: Class[_ <: CompressionCodec] =
        getOutputCompressorClass(job, classOf[GzipCodec])
      val codec: CompressionCodec     = ReflectionUtils.newInstance(codecClass, conf)
      val file: Path                  = getDefaultWorkFile(job, suffix + codec.getDefaultExtension)
      val fs: FileSystem              = file.getFileSystem(conf)
      val fileOut: FSDataOutputStream = fs.create(file, false)
      val out: DataOutputStream       = new DataOutputStream(codec.createOutputStream(fileOut))
      new JacksonKeyRecordWriter(schema, out)
    } else {
      val file: Path              = getDefaultWorkFile(job, suffix)
      val fs: FileSystem          = file.getFileSystem(conf)
      val out: FSDataOutputStream = fs.create(file, false)
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
