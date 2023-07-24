package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.time.{awakeEvery, Tick}
import fs2.io.readInputStream
import fs2.text.{lines, utf8}
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.parser.parse
import io.scalaland.enumz.Enum
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy
final class NJCirce[F[_]](
  configuration: Configuration,
  blockSizeHint: Long,
  chunkSize: ChunkSize,
  compressLevel: CompressionLevel) {
  def withBlockSizeHint(bsh: Long): NJCirce[F] =
    new NJCirce[F](configuration, bsh, chunkSize, compressLevel)

  def withChunkSize(cs: ChunkSize): NJCirce[F] =
    new NJCirce[F](configuration, blockSizeHint, cs, compressLevel)
  def withCompressionLevel(cl: CompressionLevel): NJCirce[F] =
    new NJCirce[F](configuration, blockSizeHint, chunkSize, cl)

  def withCompressionLevel(level: Int): NJCirce[F] =
    withCompressionLevel(Enum[CompressionLevel].withIndex(level))

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, Json] =
    for {
      is <- Stream.resource(NJReader.inputStream[F](configuration, path))
      json <- readInputStream[F](F.pure(is), chunkSize.value)
        .through(utf8.decode)
        .through(lines)
        .filter(_.nonEmpty)
        .mapChunks(_.map(parse))
        .rethrow
    } yield json

  def source(paths: List[NJPath])(implicit F: Sync[F]): Stream[F, Json] =
    paths.foldLeft(Stream.empty.covaryAll[F, Json]) { case (s, p) =>
      s ++ source(p)
    }

  private def toBytes(ss: Stream[F, Json]): Stream[F, Byte] =
    ss.mapChunks(_.map(_.noSpaces)).intersperse(NEWLINE_SEPERATOR).through(utf8.encode)
  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Json, Nothing] = { (ss: Stream[F, Json]) =>
    Stream
      .resource(NJWriter.bytes[F](configuration, compressLevel, blockSizeHint, path))
      .flatMap(w => persist[F, Byte](w, toBytes(ss)).stream)
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Json, Nothing] = {
    def getWriter(tick: Tick): Resource[F, NJWriter[F, Byte]] =
      NJWriter.bytes[F](configuration, compressLevel, blockSizeHint, pathBuilder(tick))

    val init: Resource[F, (Hotswap[F, NJWriter[F, Byte]], NJWriter[F, Byte])] =
      Hotswap(NJWriter.bytes[F](configuration, compressLevel, blockSizeHint, pathBuilder(Tick.Zero)))

    (ss: Stream[F, Json]) =>
      Stream.resource(init).flatMap { case (hotswap, writer) =>
        rotatePersist[F, Byte](
          getWriter,
          hotswap,
          writer,
          toBytes(ss).map(Left(_)).mergeHaltL(awakeEvery[F](policy).map(Right(_)))
        ).stream
      }
  }

}
object NJCirce {
  def apply[F[_]](cfg: Configuration): NJCirce[F] =
    new NJCirce[F](cfg, BLOCK_SIZE_HINT, CHUNK_SIZE, CompressionLevel.DEFAULT_COMPRESSION)
}
