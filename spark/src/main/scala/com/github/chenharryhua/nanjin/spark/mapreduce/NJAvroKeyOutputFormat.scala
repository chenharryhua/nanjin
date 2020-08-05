package com.github.chenharryhua.nanjin.spark.mapreduce

import java.io.OutputStream

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroOutputFormatBase}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

// avro build-in(AvroKeyOutputFormat) does not support s3, yet
class NJAvroKeyOutputFormat extends AvroOutputFormatBase[AvroKey[GenericRecord], NullWritable] {

  private def fileOutputStream(context: TaskAttemptContext): OutputStream = {
    val committer = getOutputCommitter(context) match {
      case c: FileOutputCommitter  => c.getWorkPath
      case s: AbstractS3ACommitter => s.getWorkPath
      case ex                      => throw new Exception(s"not support: ${ex.toString}")
    }

    val path: Path =
      new Path(
        committer,
        FileOutputFormat
          .getUniqueFile(context, s"nj-${context.getTaskAttemptID.getJobID.toString}", ".avro"))

    path.getFileSystem(context.getConfiguration).create(path)
  }

  override def getRecordWriter(
    context: TaskAttemptContext): RecordWriter[AvroKey[GenericRecord], NullWritable] = {
    val schema: Schema    = AvroJob.getOutputKeySchema(context.getConfiguration)
    val out: OutputStream = fileOutputStream(context)
    new AvroKeyRecordWriter(schema, out)
  }
}

final class AvroKeyRecordWriter(schema: Schema, os: OutputStream)
    extends RecordWriter[AvroKey[GenericRecord], NullWritable] {

  private val datumWriter: GenericDatumWriter[GenericRecord] =
    new GenericDatumWriter[GenericRecord](schema)

  private val dataFileWriter: DataFileWriter[GenericRecord] =
    new DataFileWriter[GenericRecord](datumWriter).create(schema, os)

  override def write(key: AvroKey[GenericRecord], value: NullWritable): Unit =
    dataFileWriter.append(key.datum())

  override def close(context: TaskAttemptContext): Unit = {
    dataFileWriter.flush()
    os.close()
  }
}
