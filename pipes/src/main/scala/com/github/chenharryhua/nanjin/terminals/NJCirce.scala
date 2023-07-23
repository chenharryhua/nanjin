package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.time.{awakeEvery, Tick}
import com.github.chenharryhua.nanjin.pipes.CirceSerde
import io.circe.{Decoder, Encoder}
import io.scalaland.enumz.Enum
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import fs2.{Pipe, Stream}
import fs2.io.readInputStream
import retry.RetryPolicy
final class NJCirce[F[_], A: Encoder: Decoder](
  configuration: Configuration,
  blockSizeHint: Long,
  chunkSize: ChunkSize,
  compressLevel: CompressionLevel,
  isKeepNull: Boolean) {
  def withBlockSizeHint(bsh: Long): NJCirce[F, A] =
    new NJCirce[F, A](configuration, bsh, chunkSize, compressLevel, isKeepNull)

  def withCompressionLevel(cl: CompressionLevel): NJCirce[F, A] =
    new NJCirce[F, A](configuration, blockSizeHint, chunkSize, cl, isKeepNull)

  def withCompressionLevel(level: Int): NJCirce[F, A] =
    withCompressionLevel(Enum[CompressionLevel].withIndex(level))

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, A] =
    for {
      is <- Stream.resource(NJReader.bytes[F](configuration, path))
      as <- readInputStream[F](F.pure(is), chunkSize.value).through(CirceSerde.fromBytes)
    } yield as

  def source(paths: List[NJPath])(implicit F: Sync[F]): Stream[F, A] =
    paths.foldLeft(Stream.empty.covaryAll[F, A]) { case (s, p) =>
      s ++ source(p)
    }

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, A, Nothing] = { (ss: Stream[F, A]) =>
    Stream
      .resource(NJWriter.bytes[F](configuration, compressLevel, blockSizeHint, path))
      .flatMap(w => persist[F, Byte](w, ss.through(CirceSerde.toBytes[F, A](isKeepNull))).stream)
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit F: Async[F]): Pipe[F, A, Nothing] = {
    def getWriter(tick: Tick): Resource[F, NJWriter[F, Byte]] =
      NJWriter.bytes[F](configuration, compressLevel, blockSizeHint, pathBuilder(tick))

    val init: Resource[F, (Hotswap[F, NJWriter[F, Byte]], NJWriter[F, Byte])] =
      Hotswap(NJWriter.bytes[F](configuration, compressLevel, blockSizeHint, pathBuilder(Tick.Zero)))

    (ss: Stream[F, A]) =>
      Stream.resource(init).flatMap { case (hotswap, writer) =>
        rotatePersist[F, Byte](
          getWriter,
          hotswap,
          writer,
          ss.through(CirceSerde.toBytes[F, A](isKeepNull))
            .map(Left(_))
            .mergeHaltL(awakeEvery[F](policy).map(Right(_)))
        ).stream
      }
  }

}
object NJCirce {
  def apply[F[_], A: Encoder: Decoder](cfg: Configuration, isKeepNull: Boolean): NJCirce[F, A] =
    new NJCirce[F, A](cfg, BLOCK_SIZE_HINT, CHUNK_SIZE, CompressionLevel.DEFAULT_COMPRESSION, isKeepNull)
}
