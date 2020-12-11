package com.github.chenharryhua.nanjin.spark.persist

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.{
  getCompressOutput,
  getOutputCompressorClass
}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.util.ReflectionUtils

import java.io.DataOutputStream

final class NJBinaryOutputFormat extends FileOutputFormat[NullWritable, BytesWritable] {

  override def getRecordWriter(
    job: TaskAttemptContext): RecordWriter[NullWritable, BytesWritable] = {
    val conf: Configuration         = job.getConfiguration
    val isCompressed: Boolean       = getCompressOutput(job)
    val suffix: String              = conf.get(NJBinaryOutputFormat.suffix, "")
    val file: Path                  = getDefaultWorkFile(job, suffix)
    val fs: FileSystem              = file.getFileSystem(conf)
    val fileOut: FSDataOutputStream = fs.create(file, false)
    new NJBinaryRecordWriter(fileOut)
  }
}

object NJBinaryOutputFormat {
  val suffix: String = "nj.mapreduce.output.binoutputformat.suffix"
}

final class NJBinaryRecordWriter(out: DataOutputStream)
    extends RecordWriter[NullWritable, BytesWritable] {

  override def write(key: NullWritable, value: BytesWritable): Unit =
    out.write(value.getBytes, 0, value.getLength)

  override def close(context: TaskAttemptContext): Unit = out.close()
}
