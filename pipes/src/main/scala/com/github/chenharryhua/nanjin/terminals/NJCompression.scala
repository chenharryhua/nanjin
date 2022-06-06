package com.github.chenharryhua.nanjin.terminals

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.NJFileFormat
import io.circe.{Decoder, Encoder, Json}
import org.apache.avro.file.CodecFactory
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.util.Try

sealed trait NJCompression {
  def shortName: String
  def fileExtension: String

  final def fileName(fmt: NJFileFormat): String = fmt match {
    case NJFileFormat.Unknown    => s"${NJFileFormat.Unknown.suffix}$fileExtension"
    case NJFileFormat.Jackson    => s"${NJFileFormat.Jackson.suffix}$fileExtension"
    case NJFileFormat.Circe      => s"${NJFileFormat.Circe.suffix}$fileExtension"
    case NJFileFormat.Text       => s"${NJFileFormat.Text.suffix}$fileExtension"
    case NJFileFormat.Kantan     => s"${NJFileFormat.Kantan.suffix}$fileExtension"
    case NJFileFormat.SparkJson  => s"${NJFileFormat.SparkJson.suffix}$fileExtension"
    case NJFileFormat.SparkCsv   => s"${NJFileFormat.SparkCsv.suffix}$fileExtension"
    case NJFileFormat.BinaryAvro => s"${NJFileFormat.BinaryAvro.suffix}$fileExtension"
    case NJFileFormat.JavaObject => s"${NJFileFormat.JavaObject.suffix}$fileExtension"
    case NJFileFormat.ProtoBuf   => s"${NJFileFormat.ProtoBuf.suffix}$fileExtension"

    case NJFileFormat.Parquet => s"$shortName.${NJFileFormat.Parquet.suffix}"
    case NJFileFormat.Avro    => s"$shortName.${NJFileFormat.Avro.suffix}"
  }
}

sealed trait BinaryAvroCompression extends NJCompression {}
sealed trait CirceCompression extends NJCompression {}
sealed trait JacksonCompression extends NJCompression {}
sealed trait KantanCompression extends NJCompression {}
sealed trait SparkJsonCompression extends NJCompression {}
sealed trait TextCompression extends NJCompression {}
sealed trait ParquetCompression extends NJCompression {
  final def codecName: CompressionCodecName = this match {
    case NJCompression.Uncompressed => CompressionCodecName.UNCOMPRESSED
    case NJCompression.Snappy       => CompressionCodecName.SNAPPY
    case NJCompression.Gzip         => CompressionCodecName.GZIP
    case NJCompression.Lz4          => CompressionCodecName.LZ4
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
    case NJCompression.Xz(level)        => CodecFactory.xzCodec(level)
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
      case Brotli               => Json.fromString(Brotli.shortName)
      case Lzo                  => Json.fromString(Lzo.shortName)
      case c @ Deflate(level)   => Json.fromString(s"${c.shortName}-${level.show}") // hadoop convention
      case c @ Xz(level)        => Json.fromString(s"${c.shortName}-${level.show}")
      case c @ Zstandard(level) => Json.fromString(s"${c.shortName}-${level.show}")
    }

  implicit final val decoderNJCompression: Decoder[NJCompression] =
    Decoder[String].emap[NJCompression] {
      case Uncompressed.shortName => Right(Uncompressed)
      case Snappy.shortName       => Right(Snappy)
      case Bzip2.shortName        => Right(Bzip2)
      case Gzip.shortName         => Right(Gzip)
      case Lz4.shortName          => Right(Lz4)
      case Brotli.shortName       => Right(Brotli)
      case Lzo.shortName          => Right(Lzo)
      case s"deflate-${level}"    => Try(level.toInt).map(Deflate).toEither.leftMap(_.getMessage)
      case s"xz-${level}"         => Try(level.toInt).map(Xz).toEither.leftMap(_.getMessage)
      case s"zstd-${level}"       => Try(level.toInt).map(Zstandard).toEither.leftMap(_.getMessage)
      case unknown                => Left(s"unknown compression: $unknown")
    }

  implicit final val encoerAvroCompression: Encoder[AvroCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderAvroCompression: Decoder[AvroCompression] =
    decoderNJCompression.emap {
      case compression: AvroCompression => Right(compression)
      case unknown                      => Left(s"avro does not support: $unknown")
    }

  implicit final val encoerBinaryAvroCompression: Encoder[BinaryAvroCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderBinaryAvroCompression: Decoder[BinaryAvroCompression] =
    decoderNJCompression.emap {
      case compression: BinaryAvroCompression => Right(compression)
      case unknown                            => Left(s"binary avro does not support: $unknown")
    }

  implicit final val encoerParquetCompression: Encoder[ParquetCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderParquetCompression: Decoder[ParquetCompression] =
    decoderNJCompression.emap {
      case compression: ParquetCompression => Right(compression)
      case unknown                         => Left(s"parquet does not support: $unknown")
    }

  implicit final val encoerCirceCompression: Encoder[CirceCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderCirceCompression: Decoder[CirceCompression] =
    decoderNJCompression.emap {
      case compression: CirceCompression => Right(compression)
      case unknown                       => Left(s"circe json does not support: $unknown")
    }

  implicit final val encoerJacksonCompression: Encoder[JacksonCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderJacksonCompression: Decoder[JacksonCompression] =
    decoderNJCompression.emap {
      case compression: JacksonCompression => Right(compression)
      case unknown                         => Left(s"jackson does not support: $unknown")
    }

  implicit final val encoerKantanCompression: Encoder[KantanCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderKantanCompression: Decoder[KantanCompression] =
    decoderNJCompression.emap {
      case compression: KantanCompression => Right(compression)
      case unknown                        => Left(s"kantan csv does not support: $unknown")
    }

  implicit final val encoerSparkJsonCompression: Encoder[SparkJsonCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderSparkJsonCompression: Decoder[SparkJsonCompression] =
    decoderNJCompression.emap {
      case compression: SparkJsonCompression => Right(compression)
      case unknown                           => Left(s"spark json does not support: $unknown")
    }

  implicit final val encoerTextCompression: Encoder[TextCompression] =
    encoderNJCompression.contramap(identity)
  implicit final val decoderTextCompression: Decoder[TextCompression] =
    decoderNJCompression.emap {
      case compression: TextCompression => Right(compression)
      case unknown                      => Left(s"text does not support: $unknown")
    }

  case object Uncompressed
      extends NJCompression with AvroCompression with BinaryAvroCompression with ParquetCompression
      with CirceCompression with JacksonCompression with KantanCompression with SparkJsonCompression
      with TextCompression {
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
      with JacksonCompression with KantanCompression with SparkJsonCompression with TextCompression {
    override val shortName: String     = "bzip2"
    override val fileExtension: String = ".bz2"
  }

  case object Gzip
      extends NJCompression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with SparkJsonCompression with TextCompression {
    override val shortName: String     = "gzip"
    override val fileExtension: String = ".gz"
  }

  case object Lz4
      extends NJCompression with ParquetCompression with BinaryAvroCompression with CirceCompression
      with JacksonCompression with KantanCompression with TextCompression {
    override val shortName: String     = "lz4"
    override val fileExtension: String = ".lz4"
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
      with JacksonCompression with KantanCompression with SparkJsonCompression with TextCompression {
    override val shortName: String     = "deflate"
    override val fileExtension: String = ".deflate"
  }

  final case class Xz(level: Int) extends NJCompression with AvroCompression {
    override val shortName: String     = "xz"
    override val fileExtension: String = ".xz"
  }

  final case class Zstandard(level: Int) extends NJCompression with AvroCompression with ParquetCompression {
    override val shortName: String     = "zstd"
    override val fileExtension: String = ".zst"
  }
}
