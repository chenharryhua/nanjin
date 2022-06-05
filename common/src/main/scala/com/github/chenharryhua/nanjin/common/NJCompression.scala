package com.github.chenharryhua.nanjin.common

import io.circe.{Decoder, Encoder, Json}
import cats.syntax.all.*

import scala.util.Try

sealed trait NJCompression {
  def shortName: String
  def fileExtension: String
}

sealed trait AvroCompression extends NJCompression
sealed trait BinaryAvroCompression extends NJCompression
sealed trait CirceCompression extends NJCompression
sealed trait JacksonCompression extends NJCompression
sealed trait KantanCompression extends NJCompression
sealed trait SparkJsonCompression extends NJCompression
sealed trait TextCompression extends NJCompression
sealed trait ParquetCompression extends NJCompression

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

  case object Lzo extends NJCompression {
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
