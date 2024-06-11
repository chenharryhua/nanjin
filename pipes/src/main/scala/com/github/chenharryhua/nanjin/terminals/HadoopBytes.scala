package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.{Chunk, Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.{InputStream, OutputStream}
import java.time.ZoneId

final class HadoopBytes[F[_]] private (configuration: Configuration) {

  def source(path: NJPath, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, Byte] =
    HadoopReader.byteS(configuration, chunkSize, path.hadoopPath)

  def source(paths: List[NJPath], chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, Byte] =
    paths.foldLeft(Stream.empty.covaryAll[F, Byte]) { case (s, p) => s ++ source(p, chunkSize) }

  def inputStream(path: NJPath)(implicit F: Sync[F]): Resource[F, InputStream] =
    HadoopReader.inputStreamR[F](configuration, path.hadoopPath)

  def outputStream(path: NJPath)(implicit F: Sync[F]): Resource[F, OutputStream] =
    HadoopWriter.outputStreamR[F](path.hadoopPath, configuration)

  // write

  private def get_writerR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, Byte]] =
    HadoopWriter.byteR[F](configuration, path)

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Byte, Int] = { (ss: Stream[F, Byte]) =>
    Stream.resource(get_writerR(path.hadoopPath)).flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Byte, Int] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, Byte]] =
      get_writerR(pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, Byte]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        val ticks: Stream[F, Either[Chunk[Byte], Tick]] = tickStream[F](zero).map(Right(_))

        Stream.resource(Hotswap(getWriter(zero.tick))).flatMap { case (hotswap, writer) =>
          persist[F, Byte](
            getWriter,
            hotswap,
            writer,
            ss.chunks.map(Left(_)).mergeHaltBoth(ticks)
          ).stream
        }
      }
  }
}

object HadoopBytes {
  def apply[F[_]](cfg: Configuration): HadoopBytes[F] = new HadoopBytes[F](cfg)
}
