package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy
import squants.information.Information

final class HadoopBytes[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  bufferSize: Information,
  compressLevel: CompressionLevel) {

  // config

  def withBufferSize(bs: Information): HadoopBytes[F] =
    new HadoopBytes[F](configuration, blockSizeHint, bs, compressLevel)

  def withBlockSizeHint(bsh: Long): HadoopBytes[F] =
    new HadoopBytes[F](configuration, bsh, bufferSize, compressLevel)

  def withCompressionLevel(cl: CompressionLevel): HadoopBytes[F] =
    new HadoopBytes[F](configuration, blockSizeHint, bufferSize, cl)

  // read

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, Byte] =
    HadoopReader.byteS(configuration, bufferSize, path.hadoopPath)

  def source(paths: List[NJPath])(implicit F: Sync[F]): Stream[F, Byte] =
    paths.foldLeft(Stream.empty.covaryAll[F, Byte]) { case (s, p) =>
      s ++ source(p)
    }

  // write

  private def getWriterR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, Byte]] =
    HadoopWriter.byteR[F](configuration, compressLevel, blockSizeHint, path)

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Byte, Nothing] = { (ss: Stream[F, Byte]) =>
    Stream.resource(getWriterR(path.hadoopPath)).flatMap { os =>
      ss.chunks.foreach(os.write)
    }
  }

  // save
  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Byte, Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, Byte]] =
      getWriterR(pathBuilder(tick).hadoopPath)

    def init(tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, Byte]], HadoopWriter[F, Byte])] =
      Hotswap(getWriter(tick))

    // save
    (ss: Stream[F, Byte]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          rotatePersist[F, Byte](
            getWriter,
            hotswap,
            writer,
            ss.chunks.map(Left(_)).mergeHaltL(tickStream[F](policy, zero).map(Right(_)))
          ).stream
        }
      }
  }
}

object HadoopBytes {
  def apply[F[_]](cfg: Configuration): HadoopBytes[F] =
    new HadoopBytes[F](cfg, BLOCK_SIZE_HINT, BUFFER_SIZE, CompressionLevel.DEFAULT_COMPRESSION)
}
