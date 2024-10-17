package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.{Chunk, Pipe, Stream}
import io.lemonlabs.uri.Url
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration

import java.time.ZoneId

final class HadoopJackson[F[_]] private (configuration: Configuration, schema: Schema)
    extends HadoopSink[F, GenericRecord] {

  // read

  def source(path: Url, chunkSize: ChunkSize)(implicit F: Async[F]): Stream[F, GenericData.Record] =
    HadoopReader.jacksonS[F](configuration, schema, toHadoopPath(path), chunkSize)

  // write

  def sink(path: Url)(implicit F: Sync[F]): Pipe[F, Chunk[GenericRecord], Int] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream
        .resource(HadoopWriter.jacksonR[F](configuration, schema, toHadoopPath(path)))
        .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => Url)(implicit
    F: Async[F]): Pipe[F, Chunk[GenericRecord], Int] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, toHadoopPath(pathBuilder(tick)))

    // save
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        val ticks: Stream[F, Either[Chunk[GenericRecord], Tick]] = tickStream[F](zero).map(Right(_))

        Stream.resource(Hotswap(get_writer(zero.tick))).flatMap { case (hotswap, writer) =>
          periodically
            .persist[F, GenericRecord](
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

object HadoopJackson {
  def apply[F[_]](configuration: Configuration, schema: Schema): HadoopJackson[F] =
    new HadoopJackson[F](configuration, schema)
}
