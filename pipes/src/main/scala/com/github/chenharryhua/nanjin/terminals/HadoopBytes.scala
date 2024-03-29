package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.{Chunk, Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import squants.information.Information

import java.io.{InputStream, OutputStream}
import java.time.ZoneId

final class HadoopBytes[F[_]] private (configuration: Configuration) {

  def source(path: NJPath, bufferSize: Information)(implicit F: Sync[F]): Stream[F, Byte] =
    HadoopReader.byteS(configuration, bufferSize, path.hadoopPath)

  def source(paths: List[NJPath], bufferSize: Information)(implicit F: Sync[F]): Stream[F, Byte] =
    paths.foldLeft(Stream.empty.covaryAll[F, Byte]) { case (s, p) => s ++ source(p, bufferSize) }

  def inputStream(path: NJPath)(implicit F: Sync[F]): Resource[F, InputStream] =
    HadoopReader.inputStreamR[F](configuration, path.hadoopPath)

  def outputStream(path: NJPath)(implicit F: Sync[F]): Resource[F, OutputStream] =
    HadoopWriter.outputStreamR[F](path.hadoopPath, configuration)

  // write

  private def getWriterR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, Byte]] =
    HadoopWriter.byteR[F](configuration, path)

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[Byte], Nothing] = {
    (ss: Stream[F, Chunk[Byte]]) =>
      Stream.resource(getWriterR(path.hadoopPath)).flatMap(os => ss.foreach(os.write))
  }

  // save
  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[Byte], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, Byte]] =
      getWriterR(pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, Chunk[Byte]]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        Stream.resource(Hotswap(getWriter(zero.tick))).flatMap { case (hotswap, writer) =>
          persist[F, Byte](
            getWriter,
            hotswap,
            writer,
            ss.map(Left(_)).mergeHaltBoth(tickStream[F](zero).map(Right(_)))
          ).stream
        }
      }
  }
}

object HadoopBytes {
  def apply[F[_]](cfg: Configuration): HadoopBytes[F] = new HadoopBytes[F](cfg)
}
