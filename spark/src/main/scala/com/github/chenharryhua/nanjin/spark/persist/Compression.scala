package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.Sync
import fs2.Pipe
import fs2.compression.{deflate, gzip}
import io.scalaland.enumz
import org.apache.avro.file.{CodecFactory, DataFileConstants}
import org.apache.avro.mapred.AvroOutputFormat
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

final private[persist] case class CompressionCodecGroup[F[_]](
  klass: Class[_ <: CompressionCodec],
  name: String,
  compressionPipe: Pipe[F, Byte, Byte])

sealed private[persist] trait Compression extends Serializable {

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
    case c => throw new Exception(s"not support $c in avro")
  }

  def parquet: CompressionCodecName = this match {
    case Compression.Uncompressed => CompressionCodecName.UNCOMPRESSED
    case Compression.Snappy       => CompressionCodecName.SNAPPY
    case Compression.Gzip         => CompressionCodecName.GZIP
    case c                        => throw new Exception(s"not support $c in parquet")
  }

  def ccg[F[_]: Sync](conf: Configuration): CompressionCodecGroup[F] =
    this match {
      case Compression.Uncompressed =>
        conf.set(FileOutputFormat.COMPRESS, "false")
        CompressionCodecGroup[F](null, "uncompressed", identity)
      case Compression.Gzip =>
        conf.set(FileOutputFormat.COMPRESS, "true")
        conf.set(FileOutputFormat.COMPRESS_CODEC, classOf[GzipCodec].getName)
        CompressionCodecGroup[F](classOf[GzipCodec], "gzip", gzip[F]())
      case Compression.Deflate(level) =>
        conf.set(FileOutputFormat.COMPRESS, "true")
        conf.set(FileOutputFormat.COMPRESS_CODEC, classOf[DeflateCodec].getName)
        conf.set("zlib.compress.level", enumz.Enum[CompressionLevel].withIndex(level).toString)
        CompressionCodecGroup[F](classOf[DeflateCodec], "deflate", deflate[F](level))
      case Compression.Snappy =>
        conf.set(FileOutputFormat.COMPRESS, "true")
        conf.set(FileOutputFormat.COMPRESS_CODEC, classOf[SnappyCodec].getName)
        CompressionCodecGroup[F](classOf[SnappyCodec], "snappy", identity)
      case Compression.Bzip2 =>
        conf.set(FileOutputFormat.COMPRESS, "true")
        conf.set(FileOutputFormat.COMPRESS_CODEC, classOf[BZip2Codec].getName)
        CompressionCodecGroup[F](classOf[BZip2Codec], "bzip2", identity)
      case Compression.Zstandard(level) =>
        conf.set(FileOutputFormat.COMPRESS, "true")
        conf.set(FileOutputFormat.COMPRESS_CODEC, classOf[ZStandardCodec].getName)
        conf.set("io.compression.codec.zstd.level", level.toString)
        CompressionCodecGroup[F](classOf[ZStandardCodec], "zstd", identity)
      case c => throw new Exception(s"not support $c")
    }
}

private[persist] object Compression {

  case object Uncompressed extends Compression
  case object Snappy extends Compression
  case object Bzip2 extends Compression
  case object Gzip extends Compression

  final case class Deflate(level: Int) extends Compression
  final case class Xz(level: Int) extends Compression
  final case class Zstandard(level: Int) extends Compression
}
