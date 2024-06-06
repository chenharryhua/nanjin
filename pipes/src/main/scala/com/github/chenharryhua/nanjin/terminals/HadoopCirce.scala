package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.{Chunk, Pipe, Stream}
import io.circe.Json
import io.circe.jawn.parse
import org.apache.hadoop.conf.Configuration

import java.time.ZoneId

final class HadoopCirce[F[_]] private (configuration: Configuration) {

  // read

  def source(path: NJPath, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, Json] =
    HadoopReader.stringS(configuration, path.hadoopPath, chunkSize).mapChunks(_.map(parse)).rethrow

  def source(paths: List[NJPath], chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, Json] =
    paths.foldLeft(Stream.empty.covaryAll[F, Json]) { case (s, p) => s ++ source(p, chunkSize) }

  // write

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Json, Int] = { (ss: Stream[F, Json]) =>
    Stream.resource(HadoopWriter.stringR[F](configuration, path.hadoopPath)).flatMap { w =>
      ss.chunks.evalMap(c => w.write(c.map(_.noSpaces)).as(c.size))
    }
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Json, Int] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](configuration, pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, Json]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        val ticks: Stream[F, Either[Chunk[String], Tick]] = tickStream[F](zero).map(Right(_))
        Stream.resource(Hotswap(get_writer(zero.tick))).flatMap { case (hotswap, writer) =>
          persist[F, String](
            get_writer,
            hotswap,
            writer,
            ss.chunks.map(c => Left(c.map(_.noSpaces))).mergeHaltBoth(ticks)
          ).stream
        }
      }
  }
}

object HadoopCirce {
  def apply[F[_]](cfg: Configuration): HadoopCirce[F] = new HadoopCirce[F](cfg)
}
