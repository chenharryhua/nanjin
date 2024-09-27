package com.github.chenharryhua.nanjin.terminals

import cats.syntax.all.*
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Interval.Closed
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
    case 1 => CompressionLevel.BEST_SPEED
    case 2 => CompressionLevel.TWO
    case 3 => CompressionLevel.THREE
    case 4 => CompressionLevel.FOUR
    case 5 => CompressionLevel.FIVE
    case 6 => CompressionLevel.SIX
    case 7 => CompressionLevel.SEVEN
    case 8 => CompressionLevel.EIGHT
    case 9 => CompressionLevel.BEST_COMPRESSION
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
    case NJCompression.Deflate(level)   => convert(level.value)
    case NJCompression.Xz(level)        => convert(level.value)
    case NJCompression.Zstandard(level) => convert(level.value)
  }
}

sealed trait BinaryAvroCompression extends NJCompression {}
sealed trait CirceCompression extends NJCompression {}
sealed trait JacksonCompression extends NJCompression {}
sealed trait KantanCompression extends NJCompression {}
sealed trait TextCompression extends NJCompression {}
sealed trait ProtobufCompression extends NJCompression {}
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
    case NJCompression.Deflate(level)   => CodecFactory.deflateCodec(level.value)
    case NJCompression.Xz(level)        => CodecFactory.xzCodec(level.value)
    case NJCompression.Zstandard(level) => CodecFactory.zstandardCodec(level.value)
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
      case c @ Deflate(level)   => Json.fromString(s"${c.shortName}-${level.value.show}") // hadoop convention
      case c @ Xz(level)        => Json.fromString(s"${c.shortName}-${level.value.show}")
      case c @ Zstandard(level) => Json.fromString(s"${c.shortName}-${level.value.show}")
    }

  private def convertLevel(lvl: String): Either[String, Refined[Int, Closed[1, 9]]] =
    Try(lvl.toInt).toEither.leftMap(ExceptionUtils.getMessage).flatMap(NJCompressionLevel.from)

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
      case s"deflate-${level}"    => convertLevel(level).map(Deflate(_))
      case s"xz-${level}"         => convertLevel(level).map(Xz(_))
      case s"zstd-${level}"       => convertLevel(level).map(Zstandard(_))
      case unknown                => Left(s"unknown compression: $unknown")
    }

  implicit final val encoderAvroCompression: Encoder[AvroCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderAvroCompression: Decoder[AvroCompression] =
    decoderNJCompression.emap {
      case compression: AvroCompression => Right(compression)
      case unknown                      => Left(s"avro does not support: ${unknown.productPrefix}")
    }

  implicit final val encoderBinaryAvroCompression: Encoder[BinaryAvroCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderBinaryAvroCompression: Decoder[BinaryAvroCompression] =
    decoderNJCompression.emap {
      case compression: BinaryAvroCompression => Right(compression)
      case unknown => Left(s"binary avro does not support: ${unknown.productPrefix}")
    }

  implicit final val encoderParquetCompression: Encoder[ParquetCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderParquetCompression: Decoder[ParquetCompression] =
    decoderNJCompression.emap {
      case compression: ParquetCompression => Right(compression)
      case unknown                         => Left(s"parquet does not support: ${unknown.productPrefix}")
    }

  implicit final val encoderCirceCompression: Encoder[CirceCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderCirceCompression: Decoder[CirceCompression] =
    decoderNJCompression.emap {
      case compression: CirceCompression => Right(compression)
      case unknown                       => Left(s"circe json does not support: ${unknown.productPrefix}")
    }

  implicit final val encoderJacksonCompression: Encoder[JacksonCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderJacksonCompression: Decoder[JacksonCompression] =
    decoderNJCompression.emap {
      case compression: JacksonCompression => Right(compression)
      case unknown                         => Left(s"jackson does not support: ${unknown.productPrefix}")
    }

  implicit final val encoderKantanCompression: Encoder[KantanCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderKantanCompression: Decoder[KantanCompression] =
    decoderNJCompression.emap {
      case compression: KantanCompression => Right(compression)
      case unknown                        => Left(s"kantan csv does not support: ${unknown.productPrefix}")
    }

  implicit final val encoderTextCompression: Encoder[TextCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderTextCompression: Decoder[TextCompression] =
    decoderNJCompression.emap {
      case compression: TextCompression => Right(compression)
      case unknown                      => Left(s"text does not support: ${unknown.productPrefix}")
    }

  implicit final val encoderProtobufCompression: Encoder[ProtobufCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderProtobufCompression: Decoder[ProtobufCompression] =
    decoderNJCompression.emap {
      case compression: ProtobufCompression => Right(compression)
      case unknown                          => Left(s"protobuf does not support: ${unknown.productPrefix}")
    }

  case object Uncompressed
      extends NJCompression with AvroCompression with BinaryAvroCompression with ParquetCompression
      with CirceCompression with JacksonCompression with KantanCompression with TextCompression
      with ProtobufCompression {
    override val shortName: String     = "uncompressed"
    override val fileExtension: String = ""
  }

  case object Snappy
      extends NJCompression with AvroCompression with BinaryAvroCompression with ParquetCompression
      with CirceCompression with JacksonCompression with KantanCompression with TextCompression
      with ProtobufCompression {
    override val shortName: String     = "snappy"
    override val fileExtension: String = ".snappy"
  }

  case object Bzip2
      extends NJCompression with AvroCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression with ProtobufCompression {
    override val shortName: String     = "bzip2"
    override val fileExtension: String = ".bz2"
  }

  case object Gzip
      extends NJCompression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression with ProtobufCompression {
    override val shortName: String     = "gzip"
    override val fileExtension: String = ".gz"
  }

  case object Lz4
      extends NJCompression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression with ProtobufCompression {
    override val shortName: String     = "lz4"
    override val fileExtension: String = ".lz4"
  }

  case object Lz4_Raw
      extends NJCompression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression with ProtobufCompression {
    override val shortName: String     = "lz4raw"
    override val fileExtension: String = ".lz4raw"
  }

  case object Brotli extends NJCompression with ParquetCompression with ProtobufCompression {
    override val shortName: String     = "brotli"
    override val fileExtension: String = ".brotli"
  }

  case object Lzo extends NJCompression with ParquetCompression with ProtobufCompression {
    override val shortName: String     = "lzo"
    override val fileExtension: String = ".lzo"
  }

  final case class Deflate(level: NJCompressionLevel)
      extends NJCompression with BinaryAvroCompression with AvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression with ProtobufCompression {
    override val shortName: String     = "deflate"
    override val fileExtension: String = ".deflate"
  }

  final case class Xz(level: NJCompressionLevel)
      extends NJCompression with AvroCompression with ProtobufCompression {
    override val shortName: String     = "xz"
    override val fileExtension: String = ".xz"
  }

  final case class Zstandard(level: NJCompressionLevel)
      extends NJCompression with AvroCompression with ParquetCompression {
    override val shortName: String     = "zstd"
    override val fileExtension: String = ".zst"
  }
}

object AvroCompression {
  val Uncompressed: AvroCompression                         = NJCompression.Uncompressed
  val Snappy: AvroCompression                               = NJCompression.Snappy
  val Bzip2: AvroCompression                                = NJCompression.Bzip2
  def Deflate(level: NJCompressionLevel): AvroCompression   = NJCompression.Deflate(level)
  def Xz(level: NJCompressionLevel): AvroCompression        = NJCompression.Xz(level)
  def Zstandard(level: NJCompressionLevel): AvroCompression = NJCompression.Zstandard(level)
}

object BinaryAvroCompression {
  val Uncompressed: BinaryAvroCompression                       = NJCompression.Uncompressed
  val Snappy: BinaryAvroCompression                             = NJCompression.Snappy
  val Bzip2: BinaryAvroCompression                              = NJCompression.Bzip2
  val Gzip: BinaryAvroCompression                               = NJCompression.Gzip
  val Lz4: BinaryAvroCompression                                = NJCompression.Lz4
  val Lz4_Raw: BinaryAvroCompression                            = NJCompression.Lz4_Raw
  def Deflate(level: NJCompressionLevel): BinaryAvroCompression = NJCompression.Deflate(level)
}

object JacksonCompression {
  val Uncompressed: JacksonCompression                       = NJCompression.Uncompressed
  val Snappy: JacksonCompression                             = NJCompression.Snappy
  val Bzip2: JacksonCompression                              = NJCompression.Bzip2
  val Gzip: JacksonCompression                               = NJCompression.Gzip
  val Lz4: JacksonCompression                                = NJCompression.Lz4
  val Lz4_Raw: JacksonCompression                            = NJCompression.Lz4_Raw
  def Deflate(level: NJCompressionLevel): JacksonCompression = NJCompression.Deflate(level)
}

object CirceCompression {
  val Uncompressed: CirceCompression                       = NJCompression.Uncompressed
  val Snappy: CirceCompression                             = NJCompression.Snappy
  val Bzip2: CirceCompression                              = NJCompression.Bzip2
  val Gzip: CirceCompression                               = NJCompression.Gzip
  val Lz4: CirceCompression                                = NJCompression.Lz4
  val Lz4_Raw: CirceCompression                            = NJCompression.Lz4_Raw
  def Deflate(level: NJCompressionLevel): CirceCompression = NJCompression.Deflate(level)
}

object KantanCompression {
  val Uncompressed: KantanCompression                       = NJCompression.Uncompressed
  val Snappy: KantanCompression                             = NJCompression.Snappy
  val Bzip2: KantanCompression                              = NJCompression.Bzip2
  val Gzip: KantanCompression                               = NJCompression.Gzip
  val Lz4: KantanCompression                                = NJCompression.Lz4
  val Lz4_Raw: KantanCompression                            = NJCompression.Lz4_Raw
  def Deflate(level: NJCompressionLevel): KantanCompression = NJCompression.Deflate(level)
}

object TextCompression {
  val Uncompressed: TextCompression                       = NJCompression.Uncompressed
  val Snappy: TextCompression                             = NJCompression.Snappy
  val Bzip2: TextCompression                              = NJCompression.Bzip2
  val Gzip: TextCompression                               = NJCompression.Gzip
  val Lz4: TextCompression                                = NJCompression.Lz4
  val Lz4_Raw: TextCompression                            = NJCompression.Lz4_Raw
  def Deflate(level: NJCompressionLevel): TextCompression = NJCompression.Deflate(level)
}

object ParquetCompression {
  val Uncompressed: ParquetCompression                         = NJCompression.Uncompressed
  val Snappy: ParquetCompression                               = NJCompression.Snappy
  val Gzip: ParquetCompression                                 = NJCompression.Gzip
  val Lz4: ParquetCompression                                  = NJCompression.Lz4
  val Lz4_Raw: ParquetCompression                              = NJCompression.Lz4_Raw
  val Brotli: ParquetCompression                               = NJCompression.Brotli
  val Lzo: ParquetCompression                                  = NJCompression.Lzo
  def Zstandard(level: NJCompressionLevel): ParquetCompression = NJCompression.Zstandard(level)
}

object ProtobufCompression {
  val Uncompressed: ProtobufCompression                       = NJCompression.Uncompressed
  val Snappy: ProtobufCompression                             = NJCompression.Snappy
  val Bzip2: ProtobufCompression                              = NJCompression.Bzip2
  val Gzip: ProtobufCompression                               = NJCompression.Gzip
  val Lz4: ProtobufCompression                                = NJCompression.Lz4
  val Lz4_Raw: ProtobufCompression                            = NJCompression.Lz4_Raw
  val Brotli: ProtobufCompression                             = NJCompression.Brotli
  val Lzo: ProtobufCompression                                = NJCompression.Lzo
  def Deflate(level: NJCompressionLevel): ProtobufCompression = NJCompression.Deflate(level)
  def Xz(level: NJCompressionLevel): ProtobufCompression      = NJCompression.Xz(level)
}
