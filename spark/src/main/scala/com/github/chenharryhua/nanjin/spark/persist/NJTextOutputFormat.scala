package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.terminals.FileFormat
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
import scala.util.Try

final private class NJTextOutputFormat extends FileOutputFormat[NullWritable, Text] {

  @SuppressWarnings(Array("NullParameter"))
  override def checkOutputSpecs(job: JobContext): Unit = {
    val outDir = FileOutputFormat.getOutputPath(job)
    if (outDir eq null) throw new InvalidJobConfException("Output directory not set.") // scalafix:ok
    TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job.getConfiguration)
  }

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[NullWritable, Text] = {
    val conf: Configuration = job.getConfiguration
    val suffix: String = s"-${uuidStr(job)}.${conf.get(NJTextOutputFormat.suffix, FileFormat.Text.suffix)}"
    val dos: DataOutputStream = if (FileOutputFormat.getCompressOutput(job)) {
      val codecClass: Class[? <: CompressionCodec] =
        FileOutputFormat.getOutputCompressorClass(job, classOf[GzipCodec])
      val codec: CompressionCodec = ReflectionUtils.newInstance(codecClass, conf)
      val file: Path = getDefaultWorkFile(job, suffix + codec.getDefaultExtension)
      val fs: FileSystem = file.getFileSystem(conf)
      val fileOut: FSDataOutputStream = fs.create(file, false)
      new DataOutputStream(codec.createOutputStream(fileOut))
    } else {
      val file: Path = getDefaultWorkFile(job, suffix)
      val fs: FileSystem = file.getFileSystem(conf)
      fs.create(file, false)
    }

    Try(new NJTextRecordWriter(dos)).fold(handleException(dos), identity)
  }
}

object NJTextOutputFormat {
  val suffix: String = "nj.mapreduce.output.textoutputformat.suffix"
}

final private class NJTextRecordWriter(out: DataOutputStream) extends RecordWriter[NullWritable, Text] {
  override def write(key: NullWritable, value: Text): Unit = out.write(value.getBytes, 0, value.getLength)
  override def close(context: TaskAttemptContext): Unit = out.close()
}
