package com.github.chenharryhua.nanjin.common

import io.circe.{Decoder, Encoder, Json}
import cats.syntax.all.*

import scala.util.Try

sealed trait NJCompression {
  def shortName: String
  def fileExtension: String
}

object NJCompression {

  implicit final val encoderNJCompression: Encoder[NJCompression] = Encoder.instance[NJCompression] {
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

  implicit final val decoderNJCompression: Decoder[NJCompression] = Decoder[String].emap[NJCompression] {
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

  case object Uncompressed extends NJCompression {
    override val shortName: String     = "uncompressed"
    override val fileExtension: String = ""
  }

  case object Snappy extends NJCompression {
    override val shortName: String     = "snappy"
    override val fileExtension: String = ".snappy"
  }

  case object Bzip2 extends NJCompression {
    override val shortName: String     = "bzip2"
    override val fileExtension: String = ".bz2"
  }

  case object Gzip extends NJCompression {
    override val shortName: String     = "gzip"
    override val fileExtension: String = ".gz"
  }

  case object Lz4 extends NJCompression {
    override val shortName: String     = "lz4"
    override val fileExtension: String = ".lz4"
  }

  case object Brotli extends NJCompression {
    override val shortName: String     = "brotli"
    override val fileExtension: String = ".brotli"
  }

  case object Lzo extends NJCompression {
    override val shortName: String     = "lzo"
    override def fileExtension: String = ".lzo"
  }

  final case class Deflate(level: Int) extends NJCompression {
    override val shortName: String     = "deflate"
    override val fileExtension: String = ".deflate"
  }

  final case class Xz(level: Int) extends NJCompression {
    override val shortName: String     = "xz"
    override val fileExtension: String = ".xz"
  }

  final case class Zstandard(level: Int) extends NJCompression {
    override val shortName: String     = "zstd"
    override val fileExtension: String = ".zst"
  }
}
