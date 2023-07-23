package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Stream}
import io.scalaland.enumz.Enum
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import squants.information.Information

final class NJBytes[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  bufferSize: Information,
  compressLevel: CompressionLevel) {
  def withBufferSize(bs: Information): NJBytes[F] =
    new NJBytes[F](configuration, blockSizeHint, bs, compressLevel)
  def withBlockSizeHint(bsh: Long): NJBytes[F] = new NJBytes[F](configuration, bsh, bufferSize, compressLevel)
  def withCompressionLevel(cl: CompressionLevel) =
    new NJBytes[F](configuration, blockSizeHint, bufferSize, cl)
  def withCompressionLevel(level: Int): NJBytes[F] = withCompressionLevel(
    Enum[CompressionLevel].withIndex(level))

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, Byte] =
    readInputStream[F](
      F.blocking(fileInputStream(path, configuration)),
      bufferSize.toBytes.toInt,
      closeAfterUse = true
    )

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Byte, Nothing] = { (ss: Stream[F, Byte]) =>
    ss.through(
      writeOutputStream(
        F.blocking(fileOutputStream(path, configuration, compressLevel, blockSizeHint)),
        closeAfterUse = true))
  }
}

object NJBytes {
  def apply[F[_]](cfg: Configuration): NJBytes[F] =
    new NJBytes[F](cfg, BLOCK_SIZE_HINT, BUFFER_SIZE, CompressionLevel.DEFAULT_COMPRESSION)
}
