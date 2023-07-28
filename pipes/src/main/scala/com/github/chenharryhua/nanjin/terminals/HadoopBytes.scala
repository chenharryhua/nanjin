package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import fs2.io.readInputStream
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import squants.information.Information

final class HadoopBytes[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  bufferSize: Information,
  compressLevel: CompressionLevel) {
  def withBufferSize(bs: Information): HadoopBytes[F] =
    new HadoopBytes[F](configuration, blockSizeHint, bs, compressLevel)
  def withBlockSizeHint(bsh: Long): HadoopBytes[F] =
    new HadoopBytes[F](configuration, bsh, bufferSize, compressLevel)
  def withCompressionLevel(cl: CompressionLevel) =
    new HadoopBytes[F](configuration, blockSizeHint, bufferSize, cl)

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, Byte] =
    Stream
      .resource(HadoopReader.inputStream(configuration, path.hadoopPath))
      .flatMap(is => readInputStream[F](F.pure(is), bufferSize.toBytes.toInt, closeAfterUse = true))

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Byte, Nothing] = { (ss: Stream[F, Byte]) =>
    Stream
      .resource(HadoopWriter.bytes[F](configuration, compressLevel, blockSizeHint, path.hadoopPath))
      .flatMap { os =>
        ss.chunks.foreach(os.write)
      }
  }
}

object HadoopBytes {
  def apply[F[_]](cfg: Configuration): HadoopBytes[F] =
    new HadoopBytes[F](cfg, BLOCK_SIZE_HINT, BUFFER_SIZE, CompressionLevel.DEFAULT_COMPRESSION)
}
