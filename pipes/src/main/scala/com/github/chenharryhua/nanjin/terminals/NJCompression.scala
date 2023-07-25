package com.github.chenharryhua.nanjin.terminals

import cats.syntax.all.*
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Interval.Closed
import eu.timepit.refined.refineV
import io.circe.{Decoder, Encoder, Json}
import org.apache.avro.file.CodecFactory
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.util.Try

sealed trait NJCompression extends Product with Serializable {
  def shortName: String
  def fileExtension: String

  final def fileName(fmt: NJFileFormat): String = fmt match {
    case NJFileFormat.Unknown    => s"${NJFileFormat.Unknown.suffix}$fileExtension"
    case NJFileFormat.Jackson    => s"${NJFileFormat.Jackson.suffix}$fileExtension"
    case NJFileFormat.Circe      => s"${NJFileFormat.Circe.suffix}$fileExtension"
    case NJFileFormat.Text       => s"${NJFileFormat.Text.suffix}$fileExtension"
    case NJFileFormat.Kantan     => s"${NJFileFormat.Kantan.suffix}$fileExtension"
    case NJFileFormat.BinaryAvro => s"${NJFileFormat.BinaryAvro.suffix}$fileExtension"
    case NJFileFormat.JavaObject => s"${NJFileFormat.JavaObject.suffix}$fileExtension"
    case NJFileFormat.ProtoBuf   => s"${NJFileFormat.ProtoBuf.suffix}$fileExtension"

    case NJFileFormat.Parquet => s"$shortName.${NJFileFormat.Parquet.suffix}"
    case NJFileFormat.Avro    => s"$shortName.${NJFileFormat.Avro.suffix}"
  }

  private def convert(level: Int): CompressionLevel = level match {
    case 0 => CompressionLevel.NO_COMPRESSION
    case 1 => CompressionLevel.BEST_SPEED
    case 2 => CompressionLevel.TWO
    case 3 => CompressionLevel.THREE
    case 4 => CompressionLevel.FOUR
    case 5 => CompressionLevel.FIVE
    case 6 => CompressionLevel.SIX
    case 7 => CompressionLevel.SEVEN
    case 8 => CompressionLevel.EIGHT
    case 9 => CompressionLevel.BEST_SPEED
    case _ => CompressionLevel.DEFAULT_COMPRESSION
  }

  final def compressionLevel: CompressionLevel = this match {
    case NJCompression.Uncompressed     => CompressionLevel.NO_COMPRESSION
    case NJCompression.Snappy           => CompressionLevel.DEFAULT_COMPRESSION
    case NJCompression.Bzip2            => CompressionLevel.DEFAULT_COMPRESSION
    case NJCompression.Gzip             => CompressionLevel.DEFAULT_COMPRESSION
    case NJCompression.Lz4              => CompressionLevel.DEFAULT_COMPRESSION
    case NJCompression.Lz4_Raw          => CompressionLevel.DEFAULT_COMPRESSION
    case NJCompression.Brotli           => CompressionLevel.DEFAULT_COMPRESSION
    case NJCompression.Lzo              => CompressionLevel.DEFAULT_COMPRESSION
    case NJCompression.Deflate(level)   => convert(level)
    case NJCompression.Xz(level)        => convert(level.value)
    case NJCompression.Zstandard(level) => convert(level)
  }
}

sealed trait BinaryAvroCompression extends NJCompression {}
sealed trait CirceCompression extends NJCompression {}
sealed trait JacksonCompression extends NJCompression {}
sealed trait KantanCompression extends NJCompression {}
sealed trait TextCompression extends NJCompression {}
sealed trait ParquetCompression extends NJCompression {
  final def codecName: CompressionCodecName = this match {
    case NJCompression.Uncompressed => CompressionCodecName.UNCOMPRESSED
    case NJCompression.Snappy       => CompressionCodecName.SNAPPY
    case NJCompression.Gzip         => CompressionCodecName.GZIP
    case NJCompression.Lz4          => CompressionCodecName.LZ4
    case NJCompression.Lz4_Raw      => CompressionCodecName.LZ4_RAW
    case NJCompression.Brotli       => CompressionCodecName.BROTLI
    case NJCompression.Lzo          => CompressionCodecName.LZO
    case NJCompression.Zstandard(_) => CompressionCodecName.ZSTD

  }
}

sealed trait AvroCompression extends NJCompression {
  final def codecFactory: CodecFactory = this match {
    case NJCompression.Uncompressed     => CodecFactory.nullCodec()
    case NJCompression.Snappy           => CodecFactory.snappyCodec()
    case NJCompression.Bzip2            => CodecFactory.bzip2Codec()
    case NJCompression.Deflate(level)   => CodecFactory.deflateCodec(level)
    case NJCompression.Xz(level)        => CodecFactory.xzCodec(level.value)
    case NJCompression.Zstandard(level) => CodecFactory.zstandardCodec(level)
  }
}

object NJCompression {

  implicit final val encoderNJCompression: Encoder[NJCompression] =
    Encoder.instance[NJCompression] {
      case Uncompressed         => Json.fromString(Uncompressed.shortName)
      case Snappy               => Json.fromString(Snappy.shortName)
      case Bzip2                => Json.fromString(Bzip2.shortName)
      case Gzip                 => Json.fromString(Gzip.shortName)
      case Lz4                  => Json.fromString(Lz4.shortName)
      case Lz4_Raw              => Json.fromString(Lz4_Raw.shortName)
      case Brotli               => Json.fromString(Brotli.shortName)
      case Lzo                  => Json.fromString(Lzo.shortName)
      case c @ Deflate(level)   => Json.fromString(s"${c.shortName}-${level.show}") // hadoop convention
      case c @ Xz(level)        => Json.fromString(s"${c.shortName}-${level.value.show}")
      case c @ Zstandard(level) => Json.fromString(s"${c.shortName}-${level.show}")
    }

  implicit final val decoderNJCompression: Decoder[NJCompression] =
    Decoder[String].emap[NJCompression] {
      case Uncompressed.shortName => Right(Uncompressed)
      case Snappy.shortName       => Right(Snappy)
      case Bzip2.shortName        => Right(Bzip2)
      case Gzip.shortName         => Right(Gzip)
      case Lz4.shortName          => Right(Lz4)
      case Lz4_Raw.shortName      => Right(Lz4_Raw)
      case Brotli.shortName       => Right(Brotli)
      case Lzo.shortName          => Right(Lzo)
      case s"deflate-${level}"    => Try(level.toInt).map(Deflate).toEither.leftMap(_.getMessage)
      case s"xz-${level}" =>
        Try(level.toInt).toEither
          .leftMap(ex => ExceptionUtils.getMessage(ex))
          .flatMap(lvl => refineV[Closed[1, 9]](lvl))
          .map(Xz)
      case s"zstd-${level}" => Try(level.toInt).map(Zstandard).toEither.leftMap(_.getMessage)
      case unknown          => Left(s"unknown compression: $unknown")
    }

  implicit final val encoderAvroCompression: Encoder[AvroCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderAvroCompression: Decoder[AvroCompression] =
    decoderNJCompression.emap {
      case compression: AvroCompression => Right(compression)
      case unknown                      => Left(s"avro does not support: $unknown")
    }

  implicit final val encoderBinaryAvroCompression: Encoder[BinaryAvroCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderBinaryAvroCompression: Decoder[BinaryAvroCompression] =
    decoderNJCompression.emap {
      case compression: BinaryAvroCompression => Right(compression)
      case unknown                            => Left(s"binary avro does not support: $unknown")
    }

  implicit final val encoderParquetCompression: Encoder[ParquetCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderParquetCompression: Decoder[ParquetCompression] =
    decoderNJCompression.emap {
      case compression: ParquetCompression => Right(compression)
      case unknown                         => Left(s"parquet does not support: $unknown")
    }

  implicit final val encoderCirceCompression: Encoder[CirceCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderCirceCompression: Decoder[CirceCompression] =
    decoderNJCompression.emap {
      case compression: CirceCompression => Right(compression)
      case unknown                       => Left(s"circe json does not support: $unknown")
    }

  implicit final val encoderJacksonCompression: Encoder[JacksonCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderJacksonCompression: Decoder[JacksonCompression] =
    decoderNJCompression.emap {
      case compression: JacksonCompression => Right(compression)
      case unknown                         => Left(s"jackson does not support: $unknown")
    }

  implicit final val encoderKantanCompression: Encoder[KantanCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderKantanCompression: Decoder[KantanCompression] =
    decoderNJCompression.emap {
      case compression: KantanCompression => Right(compression)
      case unknown                        => Left(s"kantan csv does not support: $unknown")
    }

  implicit final val encoderTextCompression: Encoder[TextCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderTextCompression: Decoder[TextCompression] =
    decoderNJCompression.emap {
      case compression: TextCompression => Right(compression)
      case unknown                      => Left(s"text does not support: $unknown")
    }

  case object Uncompressed
      extends NJCompression with AvroCompression with BinaryAvroCompression with ParquetCompression
      with CirceCompression with JacksonCompression with KantanCompression with TextCompression {
    override val shortName: String     = "uncompressed"
    override val fileExtension: String = ""
  }

  case object Snappy
      extends NJCompression with AvroCompression with BinaryAvroCompression with ParquetCompression
      with CirceCompression with JacksonCompression with KantanCompression with TextCompression {
    override val shortName: String     = "snappy"
    override val fileExtension: String = ".snappy"
  }

  case object Bzip2
      extends NJCompression with AvroCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression {
    override val shortName: String     = "bzip2"
    override val fileExtension: String = ".bz2"
  }

  case object Gzip
      extends NJCompression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression {
    override val shortName: String     = "gzip"
    override val fileExtension: String = ".gz"
  }

  case object Lz4
      extends NJCompression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression {
    override val shortName: String     = "lz4"
    override val fileExtension: String = ".lz4"
  }

  case object Lz4_Raw
      extends NJCompression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression {
    override val shortName: String     = "lz4raw"
    override val fileExtension: String = ".lz4raw"
  }

  case object Brotli extends NJCompression with ParquetCompression {
    override val shortName: String     = "brotli"
    override val fileExtension: String = ".brotli"
  }

  case object Lzo extends NJCompression with ParquetCompression {
    override val shortName: String     = "lzo"
    override def fileExtension: String = ".lzo"
  }

  final case class Deflate(level: Int)
      extends NJCompression with BinaryAvroCompression with AvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression {
    override val shortName: String     = "deflate"
    override val fileExtension: String = ".deflate"
  }

  final case class Xz(level: Int Refined Closed[1, 9]) extends NJCompression with AvroCompression {
    override val shortName: String     = "xz"
    override val fileExtension: String = ".xz"
  }

  final case class Zstandard(level: Int) extends NJCompression with AvroCompression with ParquetCompression {
    override val shortName: String     = "zstd"
    override val fileExtension: String = ".zst"
  }
}
