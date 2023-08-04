package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.text.{lines, utf8}
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.parser.parse
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy
import squants.information.Information

final class HadoopCirce[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  bufferSize: Information,
  compressLevel: CompressionLevel) {

  // config

  def withBlockSizeHint(bsh: Long): HadoopCirce[F] =
    new HadoopCirce[F](configuration, bsh, bufferSize, compressLevel)

  def withBufferSize(bs: Information): HadoopCirce[F] =
    new HadoopCirce[F](configuration, blockSizeHint, bs, compressLevel)

  def withCompressionLevel(cl: CompressionLevel): HadoopCirce[F] =
    new HadoopCirce[F](configuration, blockSizeHint, bufferSize, cl)

  // read

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, Json] =
    HadoopReader
      .byteS(configuration, bufferSize, path.hadoopPath)
      .through(utf8.decode)
      .through(lines)
      .filter(_.nonEmpty)
      .mapChunks(_.map(parse))
      .rethrow

  def source(paths: List[NJPath])(implicit F: Sync[F]): Stream[F, Json] =
    paths.foldLeft(Stream.empty.covaryAll[F, Json]) { case (s, p) =>
      s ++ source(p)
    }

  // write

  private def getWriterR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, Byte]] =
    HadoopWriter.byteR[F](configuration, compressLevel, blockSizeHint, path)

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Json, Nothing] = { (ss: Stream[F, Json]) =>
    Stream.resource(getWriterR(path.hadoopPath)).flatMap { writer =>
      persist[F, Byte](
        writer,
        ss.mapChunks(_.map(_.noSpaces)).intersperse(NEWLINE_SEPARATOR).through(utf8.encode)).stream
    }
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Json, Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, Byte]] =
      getWriterR(pathBuilder(tick).hadoopPath)

    def init(tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, Byte]], HadoopWriter[F, Byte])] =
      Hotswap(getWriter(tick))

    // save
    (ss: Stream[F, Json]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          rotatePersist[F, Byte](
            getWriter,
            hotswap,
            writer,
            ss.mapChunks(_.map(_.noSpaces))
              .intersperse(NEWLINE_SEPARATOR)
              .through(utf8.encode)
              .chunks
              .map(Left(_))
              .mergeHaltL(tickStream[F](policy, zero).map(Right(_)))
          ).stream
        }
      }
  }
}

object HadoopCirce {
  def apply[F[_]](cfg: Configuration): HadoopCirce[F] =
    new HadoopCirce[F](cfg, BLOCK_SIZE_HINT, BUFFER_SIZE, CompressionLevel.DEFAULT_COMPRESSION)
}
