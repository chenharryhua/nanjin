package com.github.chenharryhua.nanjin.spark.persist

import org.apache.avro.file.{CodecFactory, DataFileConstants}
import org.apache.avro.mapred.AvroOutputFormat
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

sealed trait NJCompression extends Serializable {
  def name: String

  final def avro(conf: Configuration): CodecFactory = this match {
    case NJCompression.Uncompressed =>
      conf.set(FileOutputFormat.COMPRESS, "false")
      conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.NULL_CODEC)
      CodecFactory.nullCodec()
    case NJCompression.Snappy =>
      conf.set(FileOutputFormat.COMPRESS, "true")
      conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC)
      CodecFactory.snappyCodec()
    case NJCompression.Bzip2 =>
      conf.set(FileOutputFormat.COMPRESS, "true")
      conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.BZIP2_CODEC)
      CodecFactory.bzip2Codec()
    case NJCompression.Deflate(v) =>
      conf.set(FileOutputFormat.COMPRESS, "true")
      conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC)
      conf.set(AvroOutputFormat.DEFLATE_LEVEL_KEY, v.toString)
      CodecFactory.deflateCodec(v)
    case NJCompression.Xz(v) =>
      conf.set(FileOutputFormat.COMPRESS, "true")
      conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.XZ_CODEC)
      conf.set(AvroOutputFormat.XZ_LEVEL_KEY, v.toString)
      CodecFactory.xzCodec(v)
    case c => throw new Exception(s"not support $c in avro")
  }

  final def parquet: CompressionCodecName = this match {
    case NJCompression.Uncompressed => CompressionCodecName.UNCOMPRESSED
    case NJCompression.Snappy       => CompressionCodecName.SNAPPY
    case NJCompression.Gzip         => CompressionCodecName.GZIP
    case NJCompression.Lz4          => CompressionCodecName.LZ4
    case NJCompression.Lzo          => CompressionCodecName.LZO
    case NJCompression.Brotli       => CompressionCodecName.BROTLI
    case NJCompression.Zstandard(_) => CompressionCodecName.ZSTD
    case c                          => throw new Exception(s"not support $c in parquet")
  }
}

object NJCompression {

  case object Uncompressed extends NJCompression {
    override val name = "uncompressed"
  }

  case object Snappy extends NJCompression {
    override val name: String = "snappy"
  }

  case object Bzip2 extends NJCompression {
    override val name: String = "bzip2"
  }

  case object Gzip extends NJCompression {
    override val name: String = "gzip"
  }

  case object Lz4 extends NJCompression {
    override val name: String = "lz4"
  }

  case object Lzo extends NJCompression {
    override val name: String = "lzo"
  }

  case object Brotli extends NJCompression {
    override val name: String = "brotli"
  }

  final case class Deflate(level: Int) extends NJCompression {
    override val name: String = "deflate"
  }

  final case class Xz(level: Int) extends NJCompression {
    override val name: String = "xz"
  }

  final case class Zstandard(level: Int) extends NJCompression {
    override val name: String = "zstd"
  }
}
