package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.{Chunk, Pipe, Stream}
import io.lemonlabs.uri.Url
import org.apache.hadoop.conf.Configuration

import java.time.ZoneId

final class HadoopText[F[_]] private (configuration: Configuration) extends HadoopSink[F, String] {

  // read

  def source(path: Url, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, String] =
    HadoopReader.stringS[F](configuration, toHadoopPath(path), chunkSize)

  // write

  def sink(path: Url)(implicit F: Sync[F]): Pipe[F, Chunk[String], Int] = { (ss: Stream[F, Chunk[String]]) =>
    Stream
      .resource(HadoopWriter.stringR[F](configuration, toHadoopPath(path)))
      .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => Url)(implicit
    F: Async[F]): Pipe[F, Chunk[String], Int] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, toHadoopPath(pathBuilder(tick)))

    // save
    (ss: Stream[F, Chunk[String]]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        val ticks: Stream[F, Either[Chunk[String], Tick]] =
          tickStream.fromTickStatus[F](zero).map(Right(_))

        Stream.resource(Hotswap(get_writer(zero.tick))).flatMap { case (hotswap, writer) =>
          periodically
            .persist[F, String](
              get_writer,
              hotswap,
              writer,
              ss.map(Left(_)).mergeHaltBoth(ticks)
            )
            .stream
        }
      }
  }
}

object HadoopText {
  def apply[F[_]](configuration: Configuration): HadoopText[F] = new HadoopText[F](configuration)
}
