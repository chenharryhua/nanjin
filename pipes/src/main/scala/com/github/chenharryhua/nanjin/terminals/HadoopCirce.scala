package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Sync}
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.{Pipe, Stream}
import io.circe.Json
import io.circe.parser.parse
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy
import squants.information.Information

final class HadoopCirce[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  bufferSize: Information,
  compressLevel: CompressionLevel) {
  def withBlockSizeHint(bsh: Long): HadoopCirce[F] =
    new HadoopCirce[F](configuration, bsh, bufferSize, compressLevel)

  def withBufferSize(bs: Information): HadoopCirce[F] =
    new HadoopCirce[F](configuration, blockSizeHint, bs, compressLevel)
  def withCompressionLevel(cl: CompressionLevel): HadoopCirce[F] =
    new HadoopCirce[F](configuration, blockSizeHint, bufferSize, cl)

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, Json] =
    new HadoopText[F](configuration, blockSizeHint, bufferSize, compressLevel)
      .source(path)
      .filter(_.nonEmpty)
      .mapChunks(_.map(parse))
      .rethrow

  def source(paths: List[NJPath])(implicit F: Sync[F]): Stream[F, Json] =
    paths.foldLeft(Stream.empty.covaryAll[F, Json]) { case (s, p) =>
      s ++ source(p)
    }

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Json, Nothing] = { (ss: Stream[F, Json]) =>
    val textSink = new HadoopText[F](configuration, blockSizeHint, bufferSize, compressLevel).sink(path)
    ss.mapChunks(_.map(_.noSpaces)).through(textSink)
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Json, Nothing] = { (ss: Stream[F, Json]) =>
    val textSink =
      new HadoopText[F](configuration, blockSizeHint, bufferSize, compressLevel).sink(policy)(pathBuilder)
    ss.mapChunks(_.map(_.noSpaces)).through(textSink)
  }
}

object HadoopCirce {
  def apply[F[_]](cfg: Configuration): HadoopCirce[F] =
    new HadoopCirce[F](cfg, BLOCK_SIZE_HINT, BUFFER_SIZE, CompressionLevel.DEFAULT_COMPRESSION)
}
