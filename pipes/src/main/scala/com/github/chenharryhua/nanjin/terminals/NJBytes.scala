package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import fs2.{Pipe, Stream}
import fs2.io.{readInputStream, writeOutputStream}
import io.scalaland.enumz.Enum
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import squants.information.Information

final class NJBytes[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  bufferSize: Information,
  compressLevel: CompressionLevel)(implicit F: Sync[F]) {
  def withBufferSize(bs: Information): NJBytes[F] =
    new NJBytes[F](configuration, blockSizeHint, bs, compressLevel)
  def withBlockSizeHint(bsh: Long): NJBytes[F] = new NJBytes[F](configuration, bsh, bufferSize, compressLevel)
  def withCompressionLevel(cl: CompressionLevel) =
    new NJBytes[F](configuration, blockSizeHint, bufferSize, cl)
  def withCompressionLevel(level: Int): NJBytes[F] = withCompressionLevel(
    Enum[CompressionLevel].withIndex(level))

  def source(path: NJPath): Stream[F, Byte] =
    for {
      is <- Stream.bracket(F.blocking(fileInputStream(path, configuration)))(r => F.blocking(r.close()))
      byte <- readInputStream[F](
        F.blocking(is),
        bufferSize.toBytes.toInt,
        closeAfterUse = false
      ) // avoid double close
    } yield byte

  def sink(path: NJPath): Pipe[F, Byte, Nothing] = { (ss: Stream[F, Byte]) =>
    Stream
      .bracket(F.blocking(fileOutputStream(path, configuration, compressLevel, blockSizeHint)))(r =>
        F.blocking(r.close()))
      .flatMap(os => ss.through(writeOutputStream(F.pure(os), closeAfterUse = false))) // avoid double close
  }

}

object NJBytes {
  def apply[F[_]: Sync](cfg: Configuration): NJBytes[F] =
    new NJBytes[F](cfg, BLOCK_SIZE_HINT, BUFFER_SIZE, CompressionLevel.DEFAULT_COMPRESSION)
}
