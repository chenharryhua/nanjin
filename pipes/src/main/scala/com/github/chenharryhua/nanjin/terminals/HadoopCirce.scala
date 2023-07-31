package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.{awakeOnPolicy, Tick}
import fs2.io.readInputStream
import fs2.text.{lines, utf8}
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.parser.parse
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy
final class HadoopCirce[F[_]](
  configuration: Configuration,
  blockSizeHint: Long,
  chunkSize: ChunkSize,
  compressLevel: CompressionLevel) {
  def withBlockSizeHint(bsh: Long): HadoopCirce[F] =
    new HadoopCirce[F](configuration, bsh, chunkSize, compressLevel)

  def withChunkSize(cs: ChunkSize): HadoopCirce[F] =
    new HadoopCirce[F](configuration, blockSizeHint, cs, compressLevel)
  def withCompressionLevel(cl: CompressionLevel): HadoopCirce[F] =
    new HadoopCirce[F](configuration, blockSizeHint, chunkSize, cl)

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, Json] =
    for {
      is <- Stream.resource(HadoopReader.inputStream[F](configuration, path.hadoopPath))
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
      .resource(HadoopWriter.bytes[F](configuration, compressLevel, blockSizeHint, path.hadoopPath))
      .flatMap(w => persist[F, Byte](w, toBytes(ss)).stream)
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Json, Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, Byte]] =
      HadoopWriter.bytes[F](configuration, compressLevel, blockSizeHint, pathBuilder(tick).hadoopPath)

    val init: Resource[F, (Hotswap[F, HadoopWriter[F, Byte]], HadoopWriter[F, Byte])] =
      Resource.eval(Tick.Zero[F]).flatMap { zero =>
        Hotswap(
          HadoopWriter.bytes[F](configuration, compressLevel, blockSizeHint, pathBuilder(zero).hadoopPath))
      }

    // save
    (ss: Stream[F, Json]) =>
      Stream.resource(init).flatMap { case (hotswap, writer) =>
        rotatePersist[F, Byte](
          getWriter,
          hotswap,
          writer,
          toBytes(ss).map(Left(_)).mergeHaltL(awakeOnPolicy[F](policy).map(Right(_)))
        ).stream
      }
  }

}
object HadoopCirce {
  def apply[F[_]](cfg: Configuration): HadoopCirce[F] =
    new HadoopCirce[F](cfg, BLOCK_SIZE_HINT, CHUNK_SIZE, CompressionLevel.DEFAULT_COMPRESSION)
}
