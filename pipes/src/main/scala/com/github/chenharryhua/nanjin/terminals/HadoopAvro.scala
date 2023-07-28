package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.time.{awakeEvery, Tick}
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import retry.RetryPolicy

import scala.jdk.CollectionConverters.*

final class HadoopAvro[F[_]] private (
  configuration: Configuration,
  schema: Schema,
  compression: AvroCompression,
  blockSizeHint: Long,
  chunkSize: ChunkSize) {

  def withCompression(compression: AvroCompression): HadoopAvro[F] =
    new HadoopAvro[F](configuration, schema, compression, blockSizeHint, chunkSize)

  def withChunkSize(cs: ChunkSize): HadoopAvro[F] =
    new HadoopAvro[F](configuration, schema, compression, blockSizeHint, cs)

  def withBlockSizeHint(bsh: Long): HadoopAvro[F] =
    new HadoopAvro[F](configuration, schema, compression, bsh, chunkSize)

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, GenericRecord] =
    for {
      dfs <- Stream.resource(HadoopReader.avro(configuration, schema, path.hadoopPath))
      gr <- Stream.fromBlockingIterator(dfs.iterator().asScala, chunkSize.value)
    } yield gr

  def source(paths: List[NJPath])(implicit F: Sync[F]): Stream[F, GenericRecord] =
    paths.foldLeft(Stream.empty.covaryAll[F, GenericRecord]) { case (s, p) =>
      s ++ source(p)
    }

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, GenericRecord, Nothing] = {
    (ss: Stream[F, GenericRecord]) =>
      Stream
        .resource(
          HadoopWriter
            .avro[F](compression.codecFactory, schema, configuration, blockSizeHint, path.hadoopPath))
        .flatMap(w => persist[F, GenericRecord](w, ss).stream)
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, GenericRecord, Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter
        .avro[F](compression.codecFactory, schema, configuration, blockSizeHint, pathBuilder(tick).hadoopPath)

    val init: Resource[F, (Hotswap[F, HadoopWriter[F, GenericRecord]], HadoopWriter[F, GenericRecord])] =
      Hotswap(
        HadoopWriter.avro[F](
          compression.codecFactory,
          schema,
          configuration,
          blockSizeHint,
          pathBuilder(Tick.Zero).hadoopPath))

    (ss: Stream[F, GenericRecord]) =>
      Stream.resource(init).flatMap { case (hotswap, writer) =>
        rotatePersist[F, GenericRecord](
          getWriter,
          hotswap,
          writer,
          ss.map(Left(_)).mergeHaltL(awakeEvery[F](policy).map(Right(_)))
        ).stream
      }
  }
}

object HadoopAvro {
  def apply[F[_]](cfg: Configuration, schema: Schema): HadoopAvro[F] =
    new HadoopAvro[F](cfg, schema, NJCompression.Uncompressed, BLOCK_SIZE_HINT, CHUNK_SIZE)
}
