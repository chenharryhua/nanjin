package com.github.chenharryhua.nanjin.spark.persist

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.hadoop.mapreduce.{JobContext, RecordWriter, TaskAttemptContext}

import java.io.DataOutputStream

final class NJBinaryOutputFormat extends FileOutputFormat[NullWritable, BytesWritable] {

  @SuppressWarnings(Array("NullParameter"))
  override def checkOutputSpecs(job: JobContext): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null) throw new InvalidJobConfException("Output directory not set.")
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job.getConfiguration)
  }

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[NullWritable, BytesWritable] = {
    val conf: Configuration         = job.getConfiguration
    val suffix: String              = s"-${utils.uuidStr(job)}${conf.get(NJBinaryOutputFormat.suffix, "")}"
    val file: Path                  = getDefaultWorkFile(job, suffix)
    val fs: FileSystem              = file.getFileSystem(conf)
    val fileOut: FSDataOutputStream = fs.create(file, false)
    new NJBinaryRecordWriter(fileOut)
  }
}

object NJBinaryOutputFormat {
  val suffix: String = "nj.mapreduce.output.binoutputformat.suffix"
}

final class NJBinaryRecordWriter(out: DataOutputStream) extends RecordWriter[NullWritable, BytesWritable] {

  override def write(key: NullWritable, value: BytesWritable): Unit =
    out.write(value.getBytes, 0, value.getLength)

  override def close(context: TaskAttemptContext): Unit = out.close()
}
