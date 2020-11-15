package com.github.chenharryhua.nanjin.spark.persist

import java.io.DataOutputStream
import java.nio.charset.StandardCharsets

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.{
  getCompressOutput,
  getOutputCompressorClass
}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.util.ReflectionUtils

final class NJTextOutputFormat extends FileOutputFormat[NullWritable, Text] {

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[NullWritable, Text] = {
    val conf         = job.getConfiguration
    val isCompressed = getCompressOutput(job)
    val suffix       = conf.get(NJTextOutputFormat.suffix, "")
    if (isCompressed) {
      val codecClass = getOutputCompressorClass(job, classOf[GzipCodec])
      val codec      = ReflectionUtils.newInstance(codecClass, conf)
      val ext        = suffix + codec.getDefaultExtension
      val file       = getDefaultWorkFile(job, ext)
      val fs         = file.getFileSystem(conf)
      val fileOut    = fs.create(file, false)
      new NJTextRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)))
    } else {
      val file    = getDefaultWorkFile(job, suffix)
      val fs      = file.getFileSystem(conf)
      val fileOut = fs.create(file, false)
      new NJTextRecordWriter(fileOut)
    }
  }
}

object NJTextOutputFormat {
  val suffix: String = "nj.mapreduce.output.textoutputformat.suffix"
}

final class NJTextRecordWriter(out: DataOutputStream) extends RecordWriter[NullWritable, Text] {
  private val NEWLINE: Array[Byte] = "\n".getBytes(StandardCharsets.UTF_8)

  override def write(key: NullWritable, value: Text): Unit = {
    out.write(value.getBytes, 0, value.getLength)
    out.write(NEWLINE)
  }

  override def close(context: TaskAttemptContext): Unit = out.close()
}
