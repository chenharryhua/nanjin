package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.time.{awakeEvery, Tick}
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.hadoop.conf.Configuration
import retry.RetryPolicy

import scala.jdk.CollectionConverters.*

final class NJAvro[F[_]] private (
  configuration: Configuration,
  schema: Schema,
  codecFactory: CodecFactory,
  blockSizeHint: Long,
  chunkSize: ChunkSize)(implicit F: Async[F]) {

  def withCodecFactory(cf: CodecFactory): NJAvro[F] =
    new NJAvro[F](configuration, schema, cf, blockSizeHint, chunkSize)

  def withChunkSize(cs: ChunkSize): NJAvro[F] =
    new NJAvro[F](configuration, schema, codecFactory, blockSizeHint, cs)

  def withBlockSizeHint(bsh: Long): NJAvro[F] =
    new NJAvro[F](configuration, schema, codecFactory, bsh, chunkSize)

  def source(path: NJPath): Stream[F, GenericRecord] =
    for {
      is <- Stream.bracket(F.blocking(path.hadoopInputFile(configuration).newStream()))(r =>
        F.blocking(r.close()))
      dfs <- Stream.bracket[F, DataFileStream[GenericRecord]](
        F.blocking(new DataFileStream(is, new GenericDatumReader(schema))))(r => F.blocking(r.close()))
      gr <- Stream.fromBlockingIterator(dfs.iterator().asScala, chunkSize.value)
    } yield gr

  def sink(path: NJPath): Pipe[F, GenericRecord, Nothing] = { (ss: Stream[F, GenericRecord]) =>
    Stream
      .resource(NJWriter.avro[F](codecFactory, schema, configuration, blockSizeHint, path))
      .flatMap(w => persistGenericRecord[F](ss, w).stream)
  }

  def rotateSink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath): Pipe[F, GenericRecord, Nothing] = {
    def go(
      hotswap: Hotswap[F, NJWriter[F, GenericRecord]],
      grs: Stream[F, Either[GenericRecord, Tick]],
      writer: NJWriter[F, GenericRecord]): Pull[F, Nothing, Unit] =
      grs.pull.uncons.flatMap {
        case Some((head, tail)) =>
          val (data, ticks) = head.partitionEither(identity)
          ticks.last match {
            case Some(t) =>
              val newWriter =
                hotswap.swap(
                  NJWriter.avro[F](codecFactory, schema, configuration, blockSizeHint, pathBuilder(t)))
              Pull.eval(newWriter).flatMap { writer =>
                Pull.eval(writer.write(data)) >> go(hotswap, tail, writer)
              }
            case None =>
              Pull.eval(writer.write(data)) >> go(hotswap, tail, writer)
          }
        case None => Pull.done
      }

    val init: Resource[F, (Hotswap[F, NJWriter[F, GenericRecord]], NJWriter[F, GenericRecord])] =
      Hotswap(NJWriter.avro[F](codecFactory, schema, configuration, blockSizeHint, pathBuilder(Tick.Zero)))

    (ss: Stream[F, GenericRecord]) =>
      Stream.resource(init).flatMap { case (hs, w) =>
        go(hs, ss.either(awakeEvery[F](policy)), w).stream
      }
  }
}

object NJAvro {
  def apply[F[_]: Async](schema: Schema, cfg: Configuration): NJAvro[F] =
    new NJAvro[F](cfg, schema, CodecFactory.nullCodec(), BLOCK_SIZE_HINT, CHUNK_SIZE)
}
