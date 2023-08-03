package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

final class HadoopProtobuf[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  chunkSize: ChunkSize,
  compressLevel: CompressionLevel
) {

  def withBlockSizeHint(bsh: Long): HadoopProtobuf[F] =
    new HadoopProtobuf[F](configuration, bsh, chunkSize, compressLevel)

  def withChunkSize(cs: ChunkSize): HadoopProtobuf[F] =
    new HadoopProtobuf[F](configuration, blockSizeHint, cs, compressLevel)

  def withCompressionLevel(cl: CompressionLevel): HadoopProtobuf[F] =
    new HadoopProtobuf[F](configuration, blockSizeHint, chunkSize, cl)

  def source[A <: GeneratedMessage](
    path: NJPath)(implicit F: Sync[F], gmc: GeneratedMessageCompanion[A]): Stream[F, A] =
    HadoopReader
      .inputStream(configuration, path.hadoopPath)
      .flatMap(is => Stream.fromIterator(gmc.streamFromDelimitedInput(is).iterator, chunkSize.value))

  def sink[A <: GeneratedMessage](path: NJPath)(implicit F: Sync[F]): Pipe[F, A, Nothing] = {
    (ss: Stream[F, A]) =>
      Stream
        .resource(
          HadoopWriter.fileOutputStreamR(path.hadoopPath, configuration, compressLevel, blockSizeHint))
        .flatMap(os => ss.map(_.writeDelimitedTo(os)))
        .drain
  }
}

object HadoopProtobuf {
  def apply[F[_]](configuration: Configuration): HadoopProtobuf[F] =
    new HadoopProtobuf[F](configuration, BLOCK_SIZE_HINT, CHUNK_SIZE, CompressionLevel.DEFAULT_COMPRESSION)
}
