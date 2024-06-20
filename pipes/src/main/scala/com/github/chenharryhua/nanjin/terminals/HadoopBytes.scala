package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.{Chunk, Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import squants.information.Bytes

import java.io.{InputStream, OutputStream}
import java.time.ZoneId

final class HadoopBytes[F[_]] private (configuration: Configuration) {

  /** @return
    *   a stream which is chunked by ''chunkSize'' except the last chunk.
    */
  def source(path: NJPath, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, Byte] =
    HadoopReader.byteS(configuration, path.hadoopPath, chunkSize)

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, Byte] =
    source(path, ChunkSize(Bytes(1024 * 512)))

  def inputStream(path: NJPath)(implicit F: Sync[F]): Resource[F, InputStream] =
    HadoopReader.inputStreamR[F](configuration, path.hadoopPath)

  def outputStream(path: NJPath)(implicit F: Sync[F]): Resource[F, OutputStream] =
    HadoopWriter.outputStreamR[F](path.hadoopPath, configuration)

  // write

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[Byte], Int] = { (ss: Stream[F, Chunk[Byte]]) =>
    Stream
      .resource(HadoopWriter.byteR[F](configuration, path.hadoopPath))
      .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[Byte], Int] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, Byte]] =
      HadoopWriter.byteR[F](configuration, pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, Chunk[Byte]]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        val ticks: Stream[F, Either[Chunk[Byte], Tick]] = tickStream[F](zero).map(Right(_))

        Stream.resource(Hotswap(get_writer(zero.tick))).flatMap { case (hotswap, writer) =>
          periodically
            .persist[F, Byte](
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

object HadoopBytes {
  def apply[F[_]](cfg: Configuration): HadoopBytes[F] = new HadoopBytes[F](cfg)
}
