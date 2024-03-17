package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.text.{lines, utf8}
import fs2.{Chunk, Pipe, Stream}
import io.circe.jawn.parse
import io.circe.{Json, ParsingFailure}
import org.apache.hadoop.conf.Configuration
import squants.information.Information

import java.nio.charset.StandardCharsets
import java.time.ZoneId

final class HadoopCirce[F[_]] private (configuration: Configuration) {

  // read

  def source(path: NJPath, bufferSize: Information)(implicit
    F: Sync[F]): Stream[F, Either[ParsingFailure, Json]] =
    HadoopReader
      .byteS(configuration, bufferSize, path.hadoopPath)
      .through(utf8.decode)
      .through(lines)
      .filter(_.nonEmpty)
      .mapChunks(_.map(parse))

  def source(paths: List[NJPath], bufferSize: Information)(implicit
    F: Sync[F]): Stream[F, Either[ParsingFailure, Json]] =
    paths.foldLeft(Stream.empty.covaryAll[F, Either[ParsingFailure, Json]]) { case (s, p) =>
      s ++ source(p, bufferSize)
    }

  // write

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[Json], Nothing] = {
    (ss: Stream[F, Chunk[Json]]) =>
      Stream.resource(HadoopWriter.byteR[F](configuration, path.hadoopPath)).flatMap { w =>
        ss.unchunks
          .mapChunks(_.map(_.noSpaces))
          .intersperse(NEWLINE_SEPARATOR)
          .through(utf8.encode)
          .chunks
          .foreach(w.write)
      }
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[Json], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](configuration, StandardCharsets.UTF_8, pathBuilder(tick).hadoopPath)

    def init(tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, String]], HadoopWriter[F, String])] =
      Hotswap(getWriter(tick))

    // save
    (ss: Stream[F, Chunk[Json]]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        Stream.resource(init(zero.tick)).flatMap { case (hotswap, writer) =>
          val ts: Stream[F, Either[Chunk[String], (Tick, Chunk[String])]] =
            tickStream[F](zero).map(t => Right((t, Chunk.empty)))

          persistString[F](
            getWriter,
            hotswap,
            writer,
            ss.map(ck => Left(ck.map(_.noSpaces))).mergeHaltBoth(ts),
            Chunk.empty
          ).stream
        }
      }
  }
}

object HadoopCirce {
  def apply[F[_]](cfg: Configuration): HadoopCirce[F] = new HadoopCirce[F](cfg)
}
