package com.github.chenharryhua.nanjin.spark.persist

import org.apache.hadoop.io.compress.GzipCodec
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
    val conf         = job.getConfiguration
    val isCompressed = getCompressOutput(job)
    val suffix       = conf.get(NJBinaryOutputFormat.suffix, "")
    if (isCompressed) {
      val codecClass = getOutputCompressorClass(job, classOf[GzipCodec])
      val codec      = ReflectionUtils.newInstance(codecClass, conf)
      val ext        = suffix + codec.getDefaultExtension
      val file       = getDefaultWorkFile(job, ext)
      val fs         = file.getFileSystem(conf)
      val fileOut    = fs.create(file, false)
      new NJBinaryRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)))
    } else {
      val file    = getDefaultWorkFile(job, suffix)
      val fs      = file.getFileSystem(conf)
      val fileOut = fs.create(file, false)
      new NJBinaryRecordWriter(fileOut)
    }
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
