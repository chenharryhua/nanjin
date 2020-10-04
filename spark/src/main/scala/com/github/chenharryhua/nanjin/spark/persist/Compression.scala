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

final private[persist] case class CompressionCodecGroup[F[_]](
  klass: Class[_ <: CompressionCodec],
  name: String,
  pipe: Pipe[F, Byte, Byte])

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
    case c => throw new Exception(s"not support $c")
  }

  def ccg[F[_]: Sync](conf: Configuration): CompressionCodecGroup[F] =
    this match {
      case Compression.Uncompressed =>
        CompressionCodecGroup[F](null, "uncompressed", identity)
      case Compression.Gzip =>
        CompressionCodecGroup[F](classOf[GzipCodec], "gzip", gzip[F]())
      case Compression.Deflate(level) =>
        conf.set("zlib.compress.level", enumz.Enum[CompressionLevel].withIndex(level).toString)
        CompressionCodecGroup[F](classOf[DeflateCodec], "deflate", deflate[F](level))
      case Compression.Snappy =>
        CompressionCodecGroup[F](classOf[SnappyCodec], "snappy", identity)
      case Compression.Bzip2 =>
        CompressionCodecGroup[F](classOf[BZip2Codec], "bzip2", identity)
      case Compression.LZ4 =>
        CompressionCodecGroup[F](classOf[Lz4Codec], "lz4", identity)
      case Compression.Zstandard(level) =>
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
  case object LZ4 extends Compression

  final case class Deflate(level: Int) extends Compression
  final case class Xz(level: Int) extends Compression
  final case class Zstandard(level: Int) extends Compression
}
