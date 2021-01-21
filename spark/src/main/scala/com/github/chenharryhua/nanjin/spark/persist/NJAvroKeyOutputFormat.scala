package com.github.chenharryhua.nanjin.spark.persist

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroOutputFormatBase.{getCompressionCodec, getSyncInterval}
import org.apache.avro.mapreduce.{AvroJob, AvroOutputFormatBase, Syncable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.getCompressOutput
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.hadoop.mapreduce.{JobContext, RecordWriter, TaskAttemptContext}

import java.io.{DataOutputStream, OutputStream}

// avro build-in(AvroKeyOutputFormat) does not support s3, yet
final class NJAvroKeyOutputFormat extends AvroOutputFormatBase[AvroKey[GenericRecord], NullWritable] {

  override def checkOutputSpecs(job: JobContext): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null) throw new InvalidJobConfException("Output directory not set.")
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job.getConfiguration)
  }

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[AvroKey[GenericRecord], NullWritable] = {
    val schema: Schema        = AvroJob.getOutputKeySchema(job.getConfiguration)
    val conf: Configuration   = job.getConfiguration
    val isCompressed: Boolean = getCompressOutput(job)
    val syncInterval: Int     = getSyncInterval(job)

    if (isCompressed) {
      val cf: CodecFactory            = getCompressionCodec(job)
      val suffix: String              = s"-${utils.uuidStr(job)}.${cf.toString.toLowerCase}.data.avro"
      val file: Path                  = getDefaultWorkFile(job, suffix)
      val fs: FileSystem              = file.getFileSystem(conf)
      val fileOut: FSDataOutputStream = fs.create(file, false)
      val out: DataOutputStream       = new DataOutputStream(fileOut)
      new AvroKeyRecordWriter(schema, out, cf, syncInterval)
    } else {
      val suffix: String          = s"-${utils.uuidStr(job)}.data.avro"
      val file: Path              = getDefaultWorkFile(job, suffix)
      val fs: FileSystem          = file.getFileSystem(conf)
      val out: FSDataOutputStream = fs.create(file, false)
      new AvroKeyRecordWriter(schema, out, CodecFactory.nullCodec(), syncInterval)
    }
  }
}

final class AvroKeyRecordWriter(schema: Schema, os: OutputStream, cf: CodecFactory, syncInterval: Int)
    extends RecordWriter[AvroKey[GenericRecord], NullWritable] with Syncable {

  private val datumWriter: GenericDatumWriter[GenericRecord] =
    new GenericDatumWriter[GenericRecord](schema)

  private val dataFileWriter: DataFileWriter[GenericRecord] =
    new DataFileWriter[GenericRecord](datumWriter).setCodec(cf).setSyncInterval(syncInterval).create(schema, os)

  override def write(key: AvroKey[GenericRecord], value: NullWritable): Unit =
    dataFileWriter.append(key.datum())

  override def close(context: TaskAttemptContext): Unit = {
    dataFileWriter.flush()
    os.close()
  }
  override def sync(): Long = dataFileWriter.sync()
}
