package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.kernel.Sync
import fs2.{compression, Pipe}
import fs2.compression.DeflateParams
import fs2.compression.DeflateParams.Level
import org.apache.avro.file.{CodecFactory, DataFileConstants}
import org.apache.avro.mapred.AvroOutputFormat
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

sealed trait Compression extends Serializable {
  def name: String

  final def avro(conf: Configuration): CodecFactory = this match {
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

  final def parquet: CompressionCodecName = this match {
    case Compression.Uncompressed => CompressionCodecName.UNCOMPRESSED
    case Compression.Snappy       => CompressionCodecName.SNAPPY
    case Compression.Gzip         => CompressionCodecName.GZIP
    case Compression.Lz4          => CompressionCodecName.LZ4
    case Compression.Lzo          => CompressionCodecName.LZO
    case Compression.Brotli       => CompressionCodecName.BROTLI
    case Compression.Zstandard(_) => CompressionCodecName.ZSTD
    case c                        => throw new Exception(s"not support $c in parquet")
  }

  final def fs2Compression[F[_]: Sync]: Pipe[F, Byte, Byte] = {
    val cps: compression.Compression[F] = fs2.compression.Compression[F]
    this match {
      case Compression.Uncompressed => identity
      case Compression.Gzip         => cps.gzip()
      case Compression.Deflate(level) =>
        val lvl: Level = level match {
          case 0 => Level.ZERO
          case 1 => Level.ONE
          case 2 => Level.TWO
          case 3 => Level.THREE
          case 4 => Level.FOUR
          case 5 => Level.FIVE
          case 6 => Level.SIX
          case 7 => Level.SEVEN
          case 8 => Level.EIGHT
          case 9 => Level.NINE
        }
        cps.deflate(DeflateParams(level = lvl))
      case c => throw new Exception(s"fs2 Stream does not support codec: $c")
    }
  }
}

object Compression {

  case object Uncompressed extends Compression {
    override val name = "uncompressed"
  }

  case object Snappy extends Compression {
    override val name: String = "snappy"
  }

  case object Bzip2 extends Compression {
    override val name: String = "bzip2"
  }

  case object Gzip extends Compression {
    override val name: String = "gzip"
  }

  case object Lz4 extends Compression {
    override val name: String = "lz4"
  }

  case object Lzo extends Compression {
    override val name: String = "lzo"
  }

  case object Brotli extends Compression {
    override val name: String = "brotli"
  }

  final case class Deflate(level: Int) extends Compression {
    override val name: String = "deflate"
  }

  final case class Xz(level: Int) extends Compression {
    override val name: String = "xz"
  }

  final case class Zstandard(level: Int) extends Compression {
    override val name: String = "zstd"
  }
}
