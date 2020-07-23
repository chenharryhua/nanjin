package com.github.chenharryhua.nanjin.spark.mapreduce

import java.io.OutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, JsonEncoder}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroOutputFormatBase}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

final class AvroJsonKeyOutputFormat
    extends AvroOutputFormatBase[AvroKey[GenericRecord], NullWritable] {

  private def fileOutputStream(context: TaskAttemptContext): OutputStream = {
    val path: Path = new Path(
      getOutputCommitter(context).asInstanceOf[FileOutputCommitter].getWorkPath,
      FileOutputFormat.getUniqueFile(context, "jackson", ".json"))

    path.getFileSystem(context.getConfiguration).create(path)
  }

  override def getRecordWriter(
    context: TaskAttemptContext): RecordWriter[AvroKey[GenericRecord], NullWritable] = {
    val schema: Schema    = AvroJob.getOutputKeySchema(context.getConfiguration)
    val out: OutputStream = fileOutputStream(context)
    new AvroJsonKeyRecordWriter(schema, out)
  }
}

final class AvroJsonKeyRecordWriter(schema: Schema, outputStream: OutputStream)
    extends RecordWriter[AvroKey[GenericRecord], NullWritable] {

  private val datumWriter: GenericDatumWriter[GenericRecord] =
    new GenericDatumWriter[GenericRecord](schema)
  private val encoder: JsonEncoder = EncoderFactory.get().jsonEncoder(schema, outputStream)

  override def write(key: AvroKey[GenericRecord], value: NullWritable): Unit =
    datumWriter.write(key.datum(), encoder)

  override def close(context: TaskAttemptContext): Unit = {
    encoder.flush()
    outputStream.close()
  }
}
