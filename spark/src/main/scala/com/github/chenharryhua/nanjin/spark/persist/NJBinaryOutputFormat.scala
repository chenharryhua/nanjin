package com.github.chenharryhua.nanjin.spark.persist

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.{getCompressOutput, getOutputCompressorClass}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.hadoop.mapreduce.{JobContext, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.util.ReflectionUtils

import java.io.DataOutputStream

final private class NJBinaryOutputFormat extends FileOutputFormat[NullWritable, BytesWritable] {

  @SuppressWarnings(Array("NullParameter"))
  override def checkOutputSpecs(job: JobContext): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null) throw new InvalidJobConfException("Output directory not set.")
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job.getConfiguration)
  }

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[NullWritable, BytesWritable] = {
    val conf: Configuration = job.getConfiguration
    val suffix: String = s"-${uuidStr(job)}.${conf.get(NJBinaryOutputFormat.suffix, "")}"
    val isCompressed: Boolean = getCompressOutput(job)
    if (isCompressed) {
      val codecClass: Class[? <: CompressionCodec] = getOutputCompressorClass(job, classOf[GzipCodec])
      val codec: CompressionCodec = ReflectionUtils.newInstance(codecClass, conf)
      val file: Path = getDefaultWorkFile(job, suffix + codec.getDefaultExtension)
      val fs: FileSystem = file.getFileSystem(conf)
      val fileOut: FSDataOutputStream = fs.create(file, false)
      val out: DataOutputStream = new DataOutputStream(codec.createOutputStream(fileOut))
      new NJBinaryRecordWriter(out)
    } else {
      val file: Path = getDefaultWorkFile(job, suffix)
      val fs: FileSystem = file.getFileSystem(conf)
      val fileOut: FSDataOutputStream = fs.create(file, false)
      new NJBinaryRecordWriter(fileOut)
    }
  }
}

object NJBinaryOutputFormat {
  val suffix: String = "nj.mapreduce.output.binoutputformat.suffix"
}

final private class NJBinaryRecordWriter(out: DataOutputStream)
    extends RecordWriter[NullWritable, BytesWritable] {

  override def write(key: NullWritable, value: BytesWritable): Unit =
    out.write(value.getBytes, 0, value.getLength)

  override def close(context: TaskAttemptContext): Unit = out.close()
}
