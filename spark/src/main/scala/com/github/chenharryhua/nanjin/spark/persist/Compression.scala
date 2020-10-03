package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.Sync
import enumeratum.EnumEntry
import fs2.Pipe
import fs2.compression.{deflate, gzip}
import io.scalaland.enumz
import org.apache.avro.file.{CodecFactory, DataFileConstants}
import org.apache.avro.mapred.AvroOutputFormat
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.apache.hadoop.io.compress.{CompressionCodec, DeflateCodec, GzipCodec}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.collection.immutable

final private[persist] case class CompressionCodecGroup[F[_]](
  klass: Class[_ <: CompressionCodec],
  name: String,
  pipe: Pipe[F, Byte, Byte])

sealed private[persist] trait Compression extends EnumEntry with Serializable {

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

  def ccg[F[_]: Sync](conf: Configuration): CompressionCodecGroup[F] = this match {
    case Compression.Uncompressed =>
      CompressionCodecGroup[F](null, "uncompressed", identity)
    case Compression.Gzip =>
      CompressionCodecGroup[F](classOf[GzipCodec], "gzip", gzip[F]())
    case Compression.Deflate(level) =>
      conf.set("zlib.compress.level", enumz.Enum[CompressionLevel].withIndex(level).toString)
      CompressionCodecGroup[F](classOf[DeflateCodec], "deflate", deflate[F](level))
    case c => throw new Exception(s"not support $c")
  }
}

private[persist] object Compression extends enumeratum.Enum[Compression] with Serializable {
  override val values: immutable.IndexedSeq[Compression] = findValues

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
