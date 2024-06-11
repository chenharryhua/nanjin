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
import org.apache.hadoop.fs.Path

import java.time.ZoneId

final class HadoopBinAvro[F[_]] private (
  configuration: Configuration,
  schema: Schema
) extends GenericRecordSink[F] {

  // read

  def source(path: NJPath, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    HadoopReader.binAvroS[F](configuration, schema, path.hadoopPath, chunkSize)

  def source(paths: List[NJPath], chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    paths.foldLeft(Stream.empty.covaryAll[F, GenericData.Record]) { case (s, p) => s ++ source(p, chunkSize) }

  // write

  private def get_writerR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    HadoopWriter.binAvroR[F](configuration, schema, path)

  override def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, GenericRecord, Int] = {
    (ss: Stream[F, GenericRecord]) =>
      Stream
        .resource(get_writerR(path.hadoopPath))
        .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  override def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, GenericRecord, Int] = {

    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      get_writerR(pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, GenericRecord]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        val ticks: Stream[F, Either[Chunk[GenericRecord], Tick]] = tickStream[F](zero).map(Right(_))

        Stream.resource(Hotswap(getWriter(zero.tick))).flatMap { case (hotswap, writer) =>
          periodically.persist[F, GenericRecord](
            getWriter,
            hotswap,
            writer,
            ss.chunks.map(Left(_)).mergeHaltBoth(ticks)
          ).stream
        }
      }
  }
}

object HadoopBinAvro {
  def apply[F[_]](configuration: Configuration, schema: Schema): HadoopBinAvro[F] =
    new HadoopBinAvro[F](configuration, schema)
}
