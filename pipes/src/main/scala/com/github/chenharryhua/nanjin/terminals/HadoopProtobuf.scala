package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

final class HadoopProtobuf[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  chunkSize: ChunkSize,
  compressLevel: CompressionLevel
) {

  // config

  def withBlockSizeHint(bsh: Long): HadoopProtobuf[F] =
    new HadoopProtobuf[F](configuration, bsh, chunkSize, compressLevel)

  def withChunkSize(cs: ChunkSize): HadoopProtobuf[F] =
    new HadoopProtobuf[F](configuration, blockSizeHint, cs, compressLevel)

  def withCompressionLevel(cl: CompressionLevel): HadoopProtobuf[F] =
    new HadoopProtobuf[F](configuration, blockSizeHint, chunkSize, cl)

  // read

  def source[A <: GeneratedMessage](
    path: NJPath)(implicit F: Sync[F], gmc: GeneratedMessageCompanion[A]): Stream[F, A] =
    HadoopReader
      .inputStreamS(configuration, path.hadoopPath)
      .flatMap(is => Stream.fromIterator(gmc.streamFromDelimitedInput(is).iterator, chunkSize.value))

  def source[A <: GeneratedMessage](
    paths: List[NJPath])(implicit F: Sync[F], gmc: GeneratedMessageCompanion[A]): Stream[F, A] =
    paths.foldLeft(Stream.empty.covaryAll[F, A]) { case (s, p) =>
      s ++ source(p)
    }

  // write

  private def getWriterR[A <: GeneratedMessage](path: Path)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, A]] =
    HadoopWriter.protobufR[F, A](configuration, compressLevel, blockSizeHint, path)

  def sink[A <: GeneratedMessage](path: NJPath)(implicit F: Sync[F]): Pipe[F, A, Nothing] = {
    (ss: Stream[F, A]) =>
      Stream.resource(getWriterR[A](path.hadoopPath)).flatMap(w => ss.chunks.foreach(w.write))
  }

  def sink[A <: GeneratedMessage](policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, A, Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, A]] =
      getWriterR[A](pathBuilder(tick).hadoopPath)

    def init(tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, A]], HadoopWriter[F, A])] =
      Hotswap(getWriter(tick))

    // save
    (ss: Stream[F, A]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          persist[F, A](
            getWriter,
            hotswap,
            writer,
            ss.chunks.map(Left(_)).mergeHaltL(tickStream[F](policy, zero).map(Right(_)))
          ).stream
        }
      }
  }
}

object HadoopProtobuf {
  def apply[F[_]](configuration: Configuration): HadoopProtobuf[F] =
    new HadoopProtobuf[F](configuration, BLOCK_SIZE_HINT, CHUNK_SIZE, CompressionLevel.DEFAULT_COMPRESSION)
}
