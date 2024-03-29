package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.text.utf8
import fs2.{Chunk, Pipe, Stream}
import org.apache.hadoop.conf.Configuration

import java.nio.charset.StandardCharsets
import java.time.ZoneId

final class HadoopText[F[_]] private (configuration: Configuration) {

  // read

  def source(path: NJPath, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, String] =
    HadoopReader.stringS[F](configuration, path.hadoopPath, chunkSize)

  def source(paths: List[NJPath], chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, String] =
    paths.foldLeft(Stream.empty.covaryAll[F, String]) { case (s, p) => s ++ source(p, chunkSize) }

  // write

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[String], Nothing] = {
    (ss: Stream[F, Chunk[String]]) =>
      Stream
        .resource(HadoopWriter.byteR[F](configuration, path.hadoopPath))
        .flatMap(w => ss.unchunks.intersperse(NEWLINE_SEPARATOR).through(utf8.encode).chunks.foreach(w.write))
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[String], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, StandardCharsets.UTF_8, pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, Chunk[String]]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        Stream.resource(Hotswap(getWriter(zero.tick))).flatMap { case (hotswap, writer) =>
          val ticks: Stream[F, Either[Chunk[String], Tick]] = tickStream[F](zero).map(Right(_))

          persistText[F](
            getWriter,
            hotswap,
            writer,
            ss.map(Left(_)).mergeHaltBoth(ticks),
            Chunk.empty
          ).stream
        }
      }
  }
}

object HadoopText {
  def apply[F[_]](configuration: Configuration): HadoopText[F] = new HadoopText[F](configuration)
}
