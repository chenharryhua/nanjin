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

sealed trait Compression extends Product {
  def shortName: String
  def fileExtension: String

  final def fileName(fmt: FileFormat): String = fmt match {
    case FileFormat.Unknown    => s"${FileFormat.Unknown.suffix}$fileExtension"
    case FileFormat.Jackson    => s"${FileFormat.Jackson.suffix}$fileExtension"
    case FileFormat.Circe      => s"${FileFormat.Circe.suffix}$fileExtension"
    case FileFormat.Text       => s"${FileFormat.Text.suffix}$fileExtension"
    case FileFormat.Kantan     => s"${FileFormat.Kantan.suffix}$fileExtension"
    case FileFormat.BinaryAvro => s"${FileFormat.BinaryAvro.suffix}$fileExtension"
    case FileFormat.JavaObject => s"${FileFormat.JavaObject.suffix}$fileExtension"
    case FileFormat.ProtoBuf   => s"${FileFormat.ProtoBuf.suffix}$fileExtension"

    case FileFormat.Parquet => s"$shortName.${FileFormat.Parquet.suffix}"
    case FileFormat.Avro    => s"$shortName.${FileFormat.Avro.suffix}"
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
    case Compression.Uncompressed     => CompressionLevel.NO_COMPRESSION
    case Compression.Snappy           => CompressionLevel.DEFAULT_COMPRESSION
    case Compression.Bzip2            => CompressionLevel.DEFAULT_COMPRESSION
    case Compression.Gzip             => CompressionLevel.DEFAULT_COMPRESSION
    case Compression.Lz4              => CompressionLevel.DEFAULT_COMPRESSION
    case Compression.Lz4_Raw          => CompressionLevel.DEFAULT_COMPRESSION
    case Compression.Brotli           => CompressionLevel.DEFAULT_COMPRESSION
    case Compression.Lzo              => CompressionLevel.DEFAULT_COMPRESSION
    case Compression.Deflate(level)   => convert(level.value)
    case Compression.Xz(level)        => convert(level.value)
    case Compression.Zstandard(level) => convert(level.value)
  }
}

sealed trait BinaryAvroCompression extends Compression {}
sealed trait CirceCompression extends Compression {}
sealed trait JacksonCompression extends Compression {}
sealed trait KantanCompression extends Compression {}
sealed trait TextCompression extends Compression {}
sealed trait ProtobufCompression extends Compression {}
sealed trait ParquetCompression extends Compression {
  final def codecName: CompressionCodecName = this match {
    case Compression.Uncompressed => CompressionCodecName.UNCOMPRESSED
    case Compression.Snappy       => CompressionCodecName.SNAPPY
    case Compression.Gzip         => CompressionCodecName.GZIP
    case Compression.Lz4          => CompressionCodecName.LZ4
    case Compression.Lz4_Raw      => CompressionCodecName.LZ4_RAW
    case Compression.Brotli       => CompressionCodecName.BROTLI
    case Compression.Lzo          => CompressionCodecName.LZO
    case Compression.Zstandard(_) => CompressionCodecName.ZSTD
  }
}

sealed trait AvroCompression extends Compression {
  final def codecFactory: CodecFactory = this match {
    case Compression.Uncompressed     => CodecFactory.nullCodec()
    case Compression.Snappy           => CodecFactory.snappyCodec()
    case Compression.Bzip2            => CodecFactory.bzip2Codec()
    case Compression.Deflate(level)   => CodecFactory.deflateCodec(level.value)
    case Compression.Xz(level)        => CodecFactory.xzCodec(level.value)
    case Compression.Zstandard(level) => CodecFactory.zstandardCodec(level.value)
  }
}

object Compression {

  implicit final val encoderNJCompression: Encoder[Compression] =
    Encoder.instance[Compression] {
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

  implicit final val decoderNJCompression: Decoder[Compression] =
    Decoder[String].emap[Compression] {
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
      extends Compression with AvroCompression with BinaryAvroCompression with ParquetCompression
      with CirceCompression with JacksonCompression with KantanCompression with TextCompression
      with ProtobufCompression {
    override val shortName: String = "uncompressed"
    override val fileExtension: String = ""
  }

  case object Snappy
      extends Compression with AvroCompression with BinaryAvroCompression with ParquetCompression
      with CirceCompression with JacksonCompression with KantanCompression with TextCompression
      with ProtobufCompression {
    override val shortName: String = "snappy"
    override val fileExtension: String = ".snappy"
  }

  case object Bzip2
      extends Compression with AvroCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression with ProtobufCompression {
    override val shortName: String = "bzip2"
    override val fileExtension: String = ".bz2"
  }

  case object Gzip
      extends Compression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression with ProtobufCompression {
    override val shortName: String = "gzip"
    override val fileExtension: String = ".gz"
  }

  case object Lz4
      extends Compression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression with ProtobufCompression {
    override val shortName: String = "lz4"
    override val fileExtension: String = ".lz4"
  }

  case object Lz4_Raw
      extends Compression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression {
    override val shortName: String = "lz4raw"
    override val fileExtension: String = ".lz4raw"
  }

  case object Brotli extends Compression with ParquetCompression {
    override val shortName: String = "brotli"
    override val fileExtension: String = ".brotli"
  }

  case object Lzo extends Compression with ParquetCompression {
    override val shortName: String = "lzo"
    override val fileExtension: String = ".lzo"
  }

  final case class Deflate(level: NJCompressionLevel)
      extends Compression with BinaryAvroCompression with AvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression with ProtobufCompression {
    override val shortName: String = "deflate"
    override val fileExtension: String = ".deflate"
  }

  final case class Xz(level: NJCompressionLevel) extends Compression with AvroCompression {
    override val shortName: String = "xz"
    override val fileExtension: String = ".xz"
  }

  final case class Zstandard(level: NJCompressionLevel)
      extends Compression with AvroCompression with ParquetCompression {
    override val shortName: String = "zstd"
    override val fileExtension: String = ".zst"
  }
}

object AvroCompression {
  val Uncompressed: AvroCompression = Compression.Uncompressed
  val Snappy: AvroCompression = Compression.Snappy
  val Bzip2: AvroCompression = Compression.Bzip2
  def Deflate(level: NJCompressionLevel): AvroCompression = Compression.Deflate(level)
  def Xz(level: NJCompressionLevel): AvroCompression = Compression.Xz(level)
  def Zstandard(level: NJCompressionLevel): AvroCompression = Compression.Zstandard(level)
}

object BinaryAvroCompression {
  val Uncompressed: BinaryAvroCompression = Compression.Uncompressed
  val Snappy: BinaryAvroCompression = Compression.Snappy
  val Bzip2: BinaryAvroCompression = Compression.Bzip2
  val Gzip: BinaryAvroCompression = Compression.Gzip
  val Lz4: BinaryAvroCompression = Compression.Lz4
  val Lz4_Raw: BinaryAvroCompression = Compression.Lz4_Raw
  def Deflate(level: NJCompressionLevel): BinaryAvroCompression = Compression.Deflate(level)
}

object JacksonCompression {
  val Uncompressed: JacksonCompression = Compression.Uncompressed
  val Snappy: JacksonCompression = Compression.Snappy
  val Bzip2: JacksonCompression = Compression.Bzip2
  val Gzip: JacksonCompression = Compression.Gzip
  val Lz4: JacksonCompression = Compression.Lz4
  val Lz4_Raw: JacksonCompression = Compression.Lz4_Raw
  def Deflate(level: NJCompressionLevel): JacksonCompression = Compression.Deflate(level)
}

object CirceCompression {
  val Uncompressed: CirceCompression = Compression.Uncompressed
  val Snappy: CirceCompression = Compression.Snappy
  val Bzip2: CirceCompression = Compression.Bzip2
  val Gzip: CirceCompression = Compression.Gzip
  val Lz4: CirceCompression = Compression.Lz4
  val Lz4_Raw: CirceCompression = Compression.Lz4_Raw
  def Deflate(level: NJCompressionLevel): CirceCompression = Compression.Deflate(level)
}

object KantanCompression {
  val Uncompressed: KantanCompression = Compression.Uncompressed
  val Snappy: KantanCompression = Compression.Snappy
  val Bzip2: KantanCompression = Compression.Bzip2
  val Gzip: KantanCompression = Compression.Gzip
  val Lz4: KantanCompression = Compression.Lz4
  val Lz4_Raw: KantanCompression = Compression.Lz4_Raw
  def Deflate(level: NJCompressionLevel): KantanCompression = Compression.Deflate(level)
}

object TextCompression {
  val Uncompressed: TextCompression = Compression.Uncompressed
  val Snappy: TextCompression = Compression.Snappy
  val Bzip2: TextCompression = Compression.Bzip2
  val Gzip: TextCompression = Compression.Gzip
  val Lz4: TextCompression = Compression.Lz4
  val Lz4_Raw: TextCompression = Compression.Lz4_Raw
  def Deflate(level: NJCompressionLevel): TextCompression = Compression.Deflate(level)
}

object ParquetCompression {
  val Uncompressed: ParquetCompression = Compression.Uncompressed
  val Snappy: ParquetCompression = Compression.Snappy
  val Gzip: ParquetCompression = Compression.Gzip
  val Lz4: ParquetCompression = Compression.Lz4
  val Lz4_Raw: ParquetCompression = Compression.Lz4_Raw
  val Brotli: ParquetCompression = Compression.Brotli
  val Lzo: ParquetCompression = Compression.Lzo
  def Zstandard(level: NJCompressionLevel): ParquetCompression = Compression.Zstandard(level)
}

object ProtobufCompression {
  val Uncompressed: ProtobufCompression = Compression.Uncompressed
  val Snappy: ProtobufCompression = Compression.Snappy
  val Bzip2: ProtobufCompression = Compression.Bzip2
  val Gzip: ProtobufCompression = Compression.Gzip
  val Lz4: ProtobufCompression = Compression.Lz4
  def Deflate(level: NJCompressionLevel): ProtobufCompression = Compression.Deflate(level)
}
