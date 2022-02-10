package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.pipes.serde.NEWLINE_BYTES_SEPERATOR
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.hadoop.mapreduce.{JobContext, RecordWriter, TaskAttemptContext}
import org.apache.hadoop.util.ReflectionUtils

import java.io.DataOutputStream

final class NJTextOutputFormat extends FileOutputFormat[NullWritable, Text] {

  @SuppressWarnings(Array("NullParameter"))
  override def checkOutputSpecs(job: JobContext): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir == null) throw new InvalidJobConfException("Output directory not set.")
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job.getConfiguration)
  }

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[NullWritable, Text] = {
    val conf: Configuration   = job.getConfiguration
    val isCompressed: Boolean = FileOutputFormat.getCompressOutput(job)
    val suffix: String        = s"-${utils.uuidStr(job)}${conf.get(NJTextOutputFormat.suffix, "")}"
    if (isCompressed) {
      val codecClass: Class[? <: CompressionCodec] = FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
      val codec: CompressionCodec                  = ReflectionUtils.newInstance(codecClass, conf)
      val file: Path                               = getDefaultWorkFile(job, suffix + codec.getDefaultExtension)
      val fs: FileSystem                           = file.getFileSystem(conf)
      val fileOut: FSDataOutputStream              = fs.create(file, false)
      new NJTextRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)))
    } else {
      val file: Path                  = getDefaultWorkFile(job, suffix)
      val fs: FileSystem              = file.getFileSystem(conf)
      val fileOut: FSDataOutputStream = fs.create(file, false)
      new NJTextRecordWriter(fileOut)
    }
  }
}

object NJTextOutputFormat {
  val suffix: String = "nj.mapreduce.output.textoutputformat.suffix"
}

final class NJTextRecordWriter(out: DataOutputStream) extends RecordWriter[NullWritable, Text] {

  override def write(key: NullWritable, value: Text): Unit = {
    out.write(value.getBytes, 0, value.getLength)
    out.write(NEWLINE_BYTES_SEPERATOR)
  }

  override def close(context: TaskAttemptContext): Unit = out.close()
}
