package com.github.chenharryhua.nanjin.common

import io.circe.generic.JsonCodec

@JsonCodec
sealed trait NJCompression {
  def shortName: String
  def fileExtension: String
}

object NJCompression {

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
