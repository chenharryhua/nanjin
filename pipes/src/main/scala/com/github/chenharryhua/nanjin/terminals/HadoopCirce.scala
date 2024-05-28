package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.text.utf8
import fs2.{Chunk, Pipe, Stream}
import io.circe.jawn.parse
import io.circe.{Json, ParsingFailure}
import org.apache.hadoop.conf.Configuration

import java.nio.charset.StandardCharsets
import java.time.ZoneId

final class HadoopCirce[F[_]] private (configuration: Configuration) {

  // read

  def source(path: NJPath, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, Either[ParsingFailure, Json]] =
    HadoopReader.stringS(configuration, path.hadoopPath, chunkSize).mapChunks(_.map(parse))

  def source(paths: List[NJPath], chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, Either[ParsingFailure, Json]] =
    paths.foldLeft(Stream.empty.covaryAll[F, Either[ParsingFailure, Json]]) { case (s, p) =>
      s ++ source(p, chunkSize)
    }

  // write

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Json, Nothing] = { (ss: Stream[F, Json]) =>
    Stream.resource(HadoopWriter.byteR[F](configuration, path.hadoopPath)).flatMap { w =>
      ss.map(_.noSpaces).intersperse(NEWLINE_SEPARATOR).through(utf8.encode).chunks.foreach(w.write)
    }
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Json, Nothing] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](configuration, StandardCharsets.UTF_8, pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, Json]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        val ticks: Stream[F, Either[Chunk[String], Tick]] = tickStream[F](zero).map(Right(_))
        Stream.resource(Hotswap(get_writer(zero.tick))).flatMap { case (hotswap, writer) =>
          persistText[F](
            get_writer,
            hotswap,
            writer,
            ss.map(_.noSpaces).chunks.map(Left(_)).mergeHaltBoth(ticks),
            Chunk.empty
          ).stream
        }
      }
  }
}

object HadoopCirce {
  def apply[F[_]](cfg: Configuration): HadoopCirce[F] = new HadoopCirce[F](cfg)
}
