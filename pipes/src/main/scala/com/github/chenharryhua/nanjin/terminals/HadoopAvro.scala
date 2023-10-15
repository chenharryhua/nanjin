package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.{Chunk, Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.time.ZoneId
import scala.jdk.CollectionConverters.*

final class HadoopAvro[F[_]] private (
  configuration: Configuration,
  schema: Schema,
  compression: AvroCompression,
  blockSizeHint: Long) {

  // config

  def withCompression(compression: AvroCompression): HadoopAvro[F] =
    new HadoopAvro[F](configuration, schema, compression, blockSizeHint)

  def withCompression(f: AvroCompression.type => AvroCompression): HadoopAvro[F] =
    withCompression(f(AvroCompression))

  def withBlockSizeHint(bsh: Long): HadoopAvro[F] =
    new HadoopAvro[F](configuration, schema, compression, bsh)

  // read

  def source(path: NJPath, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    for {
      dfs <- Stream.resource(HadoopReader.avroR(configuration, schema, path.hadoopPath))
      gr <- Stream.fromBlockingIterator(dfs.iterator().asScala, chunkSize.value)
    } yield gr

  def source(paths: List[NJPath], chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    paths.foldLeft(Stream.empty.covaryAll[F, GenericData.Record]) { case (s, p) => s ++ source(p, chunkSize) }

  // write

  private def getWriterR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, blockSizeHint, path)

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[GenericRecord], Nothing] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream.resource(getWriterR(path.hadoopPath)).flatMap(w => ss.foreach(w.write))
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[GenericRecord], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      getWriterR(pathBuilder(tick).hadoopPath)

    def init(
      tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, GenericRecord]], HadoopWriter[F, GenericRecord])] =
      Hotswap(getWriter(tick))

    // save
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        Stream.resource(init(zero.tick)).flatMap { case (hotswap, writer) =>
          persist[F, GenericRecord](
            getWriter,
            hotswap,
            writer,
            ss.map(Left(_)).mergeHaltBoth(tickStream[F](zero).map(Right(_)))
          ).stream
        }
      }
  }
}

object HadoopAvro {
  def apply[F[_]](cfg: Configuration, schema: Schema): HadoopAvro[F] =
    new HadoopAvro[F](cfg, schema, NJCompression.Uncompressed, BLOCK_SIZE_HINT)
}
