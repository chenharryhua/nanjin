package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration

import scala.jdk.CollectionConverters.*

final class NJAvro[F[_]] private (
  configuration: Configuration,
  schema: Schema,
  codecFactory: CodecFactory,
  blockSizeHint: Long,
  chunkSize: ChunkSize)(implicit F: Sync[F]) {

  def withCodecFactory(cf: CodecFactory): NJAvro[F] =
    new NJAvro[F](configuration, schema, cf, blockSizeHint, chunkSize)

  def withChunkSize(cs: ChunkSize): NJAvro[F] =
    new NJAvro[F](configuration, schema, codecFactory, blockSizeHint, cs)

  def withBlockSizeHint(bsh: Long): NJAvro[F] =
    new NJAvro[F](configuration, schema, codecFactory, bsh, chunkSize)

  def sink(path: NJPath): Pipe[F, GenericRecord, Nothing] = { (ss: Stream[F, GenericRecord]) =>
    Stream
      .resource(NJWriter.avro[F](codecFactory, schema, configuration, blockSizeHint, path))
      .flatMap(w => persistGenericRecord[F](ss, w).stream)
  }

  def source(path: NJPath): Stream[F, GenericRecord] =
    for {
      is <- Stream.bracket(F.blocking(path.hadoopInputFile(configuration).newStream()))(r =>
        F.blocking(r.close()))
      dfs <- Stream.bracket[F, DataFileStream[GenericRecord]](
        F.blocking(new DataFileStream(is, new GenericDatumReader(schema))))(r => F.blocking(r.close()))
      gr <- Stream.fromBlockingIterator(dfs.iterator().asScala, chunkSize.value)
    } yield gr

}

object NJAvro {
  def apply[F[_]: Sync](schema: Schema, cfg: Configuration): NJAvro[F] =
    new NJAvro[F](cfg, schema, CodecFactory.nullCodec(), BLOCK_SIZE_HINT, CHUNK_SIZE)
}
