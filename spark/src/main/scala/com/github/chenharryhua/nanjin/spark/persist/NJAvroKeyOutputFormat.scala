package com.github.chenharryhua.nanjin.spark.persist

import java.io.OutputStream

import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroOutputFormatBase.{getCompressionCodec, getSyncInterval}
import org.apache.avro.mapreduce.{AvroJob, AvroOutputFormatBase, Syncable}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

// avro build-in(AvroKeyOutputFormat) does not support s3, yet
final class NJAvroKeyOutputFormat
    extends AvroOutputFormatBase[AvroKey[GenericRecord], NullWritable] {

  private def fileOutputStream(context: TaskAttemptContext): OutputStream = {
    val workPath = getOutputCommitter(context) match {
      case c: FileOutputCommitter  => c.getWorkPath
      case s: AbstractS3ACommitter => s.getWorkPath
      case ex                      => throw new Exception(s"not support: ${ex.toString}")
    }
    val compression = getCompressionCodec(context).toString
    val path: Path =
      new Path(
        workPath,
        FileOutputFormat.getUniqueFile(
          context,
          s"nj-$compression-${context.getTaskAttemptID.getJobID.toString}",
          ".avro"))

    path.getFileSystem(context.getConfiguration).create(path)
  }

  override def getRecordWriter(
    context: TaskAttemptContext): RecordWriter[AvroKey[GenericRecord], NullWritable] = {
    val schema: Schema    = AvroJob.getOutputKeySchema(context.getConfiguration)
    val out: OutputStream = fileOutputStream(context)
    val compression       = getCompressionCodec(context)
    val syncInterval      = getSyncInterval(context)
    new AvroKeyRecordWriter(schema, out, compression, syncInterval)
  }
}

final class AvroKeyRecordWriter(
  schema: Schema,
  os: OutputStream,
  cf: CodecFactory,
  syncInterval: Int)
    extends RecordWriter[AvroKey[GenericRecord], NullWritable] with Syncable {

  private val datumWriter: GenericDatumWriter[GenericRecord] =
    new GenericDatumWriter[GenericRecord](schema)

  private val dataFileWriter: DataFileWriter[GenericRecord] =
    new DataFileWriter[GenericRecord](datumWriter)
      .setCodec(cf)
      .setSyncInterval(syncInterval)
      .create(schema, os)

  override def write(key: AvroKey[GenericRecord], value: NullWritable): Unit =
    dataFileWriter.append(key.datum())

  override def close(context: TaskAttemptContext): Unit = {
    dataFileWriter.flush()
    os.close()
  }

  override def sync(): Long = dataFileWriter.sync()

}
