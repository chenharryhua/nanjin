package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.Sync
import fs2.Pipe
import org.apache.avro.file.{CodecFactory, DataFileConstants}
import org.apache.avro.mapred.AvroOutputFormat
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

private[persist] trait Compression {

  def avro(conf: Configuration): CodecFactory = this match {
    case Compression.Uncompressed =>
      conf.set(FileOutputFormat.COMPRESS, "false")
      conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.NULL_CODEC)
      CodecFactory.nullCodec()
    case Compression.Snappy =>
      conf.set(FileOutputFormat.COMPRESS, "true")
      conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC)
      CodecFactory.snappyCodec()
    case Compression.Bzip2 =>
      conf.set(FileOutputFormat.COMPRESS, "true")
      conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.BZIP2_CODEC)
      CodecFactory.bzip2Codec()
    case Compression.Deflate(v) =>
      conf.set(FileOutputFormat.COMPRESS, "true")
      conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC)
      conf.set(AvroOutputFormat.DEFLATE_LEVEL_KEY, v.toString)
      CodecFactory.deflateCodec(v)
    case Compression.Xz(v) =>
      conf.set(FileOutputFormat.COMPRESS, "true")
      conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.XZ_CODEC)
      conf.set(AvroOutputFormat.XZ_LEVEL_KEY, v.toString)
      CodecFactory.xzCodec(v)
    case c => throw new Exception(s"not support $c")
  }

  def parquet: CompressionCodecName = this match {
    case Compression.Uncompressed => CompressionCodecName.UNCOMPRESSED
    case Compression.Snappy       => CompressionCodecName.SNAPPY
    case Compression.Gzip         => CompressionCodecName.GZIP
    case c                        => throw new Exception(s"not support $c")
  }

  def textCodec: String = this match {
    case Compression.Uncompressed => "uncompressed"
    case Compression.Gzip         => "gzip"
    case Compression.Bzip2        => "bzip2"
    case Compression.Deflate(_)   => "deflate"
    case c                        => s"not support $c"
  }

  def byteCompress[F[_]: Sync]: Pipe[F, Byte, Byte] = this match {
    case Compression.Uncompressed   => identity
    case Compression.Gzip           => fs2.compression.gzip[F]()
    case Compression.Deflate(level) => fs2.compression.deflate(level)
    case c                          => throw new Exception(s"not support $c")
  }
}

private[persist] object Compression {
  case object Uncompressed extends Compression
  case object Snappy extends Compression
  case object Bzip2 extends Compression
  case object Gzip extends Compression
  case object LZO extends Compression
  case object LZ4 extends Compression
  case object BROTLI extends Compression

  final case class Deflate(level: Int) extends Compression
  final case class Xz(level: Int) extends Compression
  final case class Zstandard(level: Int, useCheckSum: Boolean) extends Compression
}
