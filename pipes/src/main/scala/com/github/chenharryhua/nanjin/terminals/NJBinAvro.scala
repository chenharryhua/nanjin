package com.github.chenharryhua.nanjin.terminals
import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.time.{awakeEvery, Tick}
import com.github.chenharryhua.nanjin.pipes.BinaryAvroSerde
import fs2.io.readInputStream
import fs2.{Pipe, Stream}
import io.scalaland.enumz.Enum
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy

final class NJBinAvro[F[_]](
  configuration: Configuration,
  blockSizeHint: Long,
  chunkSize: ChunkSize,
  compressLevel: CompressionLevel,
  schema: Schema
) {

  def withBlockSizeHint(bsh: Long): NJBinAvro[F] =
    new NJBinAvro[F](configuration, bsh, chunkSize, compressLevel, schema)

  def withCompressionLevel(cl: CompressionLevel): NJBinAvro[F] =
    new NJBinAvro[F](configuration, blockSizeHint, chunkSize, cl, schema)

  def withCompressionLevel(level: Int): NJBinAvro[F] =
    withCompressionLevel(Enum[CompressionLevel].withIndex(level))

  def withChunkSize(cs: ChunkSize): NJBinAvro[F] =
    new NJBinAvro[F](configuration, blockSizeHint, cs, compressLevel, schema)

  def source(path: NJPath)(implicit F: Async[F]): Stream[F, GenericRecord] =
    for {
      is <- Stream.resource(NJReader.inputStream[F](configuration, path))
      as <- readInputStream[F](F.pure(is), chunkSize.value).through(BinaryAvroSerde.fromBytes(schema))
    } yield as

  def source(paths: List[NJPath])(implicit F: Async[F]): Stream[F, GenericRecord] =
    paths.foldLeft(Stream.empty.covaryAll[F, GenericRecord]) { case (s, p) =>
      s ++ source(p)
    }

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, GenericRecord, Nothing] = {
    (ss: Stream[F, GenericRecord]) =>
      Stream
        .resource(NJWriter.bytes[F](configuration, compressLevel, blockSizeHint, path))
        .flatMap(w => persist[F, Byte](w, ss.through(BinaryAvroSerde.toBytes(schema))).stream)
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, GenericRecord, Nothing] = {
    def getWriter(tick: Tick): Resource[F, NJWriter[F, Byte]] =
      NJWriter.bytes[F](configuration, compressLevel, blockSizeHint, pathBuilder(tick))

    val init: Resource[F, (Hotswap[F, NJWriter[F, Byte]], NJWriter[F, Byte])] =
      Hotswap(NJWriter.bytes[F](configuration, compressLevel, blockSizeHint, pathBuilder(Tick.Zero)))

    (ss: Stream[F, GenericRecord]) =>
      Stream.resource(init).flatMap { case (hotswap, writer) =>
        rotatePersist[F, Byte](
          getWriter,
          hotswap,
          writer,
          ss.through(BinaryAvroSerde.toBytes[F](schema))
            .map(Left(_))
            .mergeHaltL(awakeEvery[F](policy).map(Right(_)))
        ).stream
      }
  }
}

object NJBinAvro {
  def apply[F[_]](configuration: Configuration, schema: Schema): NJBinAvro[F] =
    new NJBinAvro[F](configuration, BLOCK_SIZE_HINT, CHUNK_SIZE, CompressionLevel.DEFAULT_COMPRESSION, schema)
}
