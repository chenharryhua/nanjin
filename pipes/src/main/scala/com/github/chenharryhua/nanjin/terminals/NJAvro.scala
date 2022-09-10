package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
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

  def withChunSize(cs: ChunkSize): NJAvro[F] =
    new NJAvro[F](configuration, schema, codecFactory, blockSizeHint, cs)

  def withBlockSizeHint(bsh: Long): NJAvro[F] =
    new NJAvro[F](configuration, schema, codecFactory, bsh, chunkSize)

  def sink(path: NJPath): Pipe[F, GenericRecord, Nothing] = {
    def go(grs: Stream[F, GenericRecord], writer: DataFileWriter[GenericRecord]): Pull[F, Nothing, Unit] =
      grs.pull.uncons.flatMap {
        case Some((hl, tl)) => Pull.eval(F.blocking(hl.foreach(writer.append))) >> go(tl, writer)
        case None           => Pull.eval(F.blocking(writer.close())) >> Pull.done
      }

    val dataFileWriter: Stream[F, DataFileWriter[GenericRecord]] = for {
      dfw <- Stream.bracket[F, DataFileWriter[GenericRecord]](
        F.blocking(new DataFileWriter(new GenericDatumWriter(schema)).setCodec(codecFactory)))(r =>
        F.blocking(r.close()))
      os <- Stream.bracket(F.blocking(path.hadoopOutputFile(configuration).createOrOverwrite(blockSizeHint)))(
        r => F.blocking(r.close()))
      writer <- Stream.bracket(F.blocking(dfw.create(schema, os)))(r => F.blocking(r.close()))
    } yield writer

    (ss: Stream[F, GenericRecord]) => dataFileWriter.flatMap(w => go(ss, w).stream)
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
