package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.{Chunk, Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration

import java.time.ZoneId

final class HadoopJackson[F[_]] private (configuration: Configuration, schema: Schema)
    extends GenericRecordSink[F] {

  // read

  def source(path: NJPath, chunkSize: ChunkSize)(implicit F: Async[F]): Stream[F, GenericData.Record] =
    HadoopReader.jacksonS[F](configuration, schema, path.hadoopPath, chunkSize)

  // write

  override def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, GenericRecord, Int] = {
    (ss: Stream[F, GenericRecord]) =>
      Stream
        .resource(HadoopWriter.jacksonR[F](configuration, schema, path.hadoopPath))
        .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  override def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, GenericRecord, Int] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter.jacksonR[F](configuration, schema, pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, GenericRecord]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        val ticks: Stream[F, Either[Chunk[GenericRecord], Tick]] = tickStream[F](zero).map(Right(_))

        Stream.resource(Hotswap(get_writer(zero.tick))).flatMap { case (hotswap, writer) =>
          periodically
            .persist[F, GenericRecord](
              get_writer,
              hotswap,
              writer,
              ss.chunks.map(Left(_)).mergeHaltBoth(ticks)
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
