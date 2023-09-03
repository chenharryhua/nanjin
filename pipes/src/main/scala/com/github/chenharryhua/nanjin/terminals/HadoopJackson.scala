package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.datetime.policies.Policy
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.{Chunk, Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel

final class HadoopJackson[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  compressLevel: CompressionLevel,
  schema: Schema) {

  // config

  def withBlockSizeHint(bsh: Long): HadoopJackson[F] =
    new HadoopJackson[F](configuration, bsh, compressLevel, schema)

  def withCompressionLevel(cl: CompressionLevel): HadoopJackson[F] =
    new HadoopJackson[F](configuration, blockSizeHint, cl, schema)

  // read

  def source(path: NJPath)(implicit F: Async[F]): Stream[F, GenericData.Record] =
    HadoopReader.jacksonS[F](configuration, schema, path.hadoopPath)

  def source(paths: List[NJPath])(implicit F: Async[F]): Stream[F, GenericData.Record] =
    paths.foldLeft(Stream.empty.covaryAll[F, GenericData.Record]) { case (s, p) => s ++ source(p) }

  // write

  private def getWriterR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    HadoopWriter.jacksonR[F](configuration, compressLevel, blockSizeHint, schema, path)

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[GenericRecord], Nothing] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream.resource(getWriterR(path.hadoopPath)).flatMap(w => ss.foreach(w.write))
  }

  def sink(policy: Policy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[GenericRecord], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      getWriterR(pathBuilder(tick).hadoopPath)

    def init(
      zero: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, GenericRecord]], HadoopWriter[F, GenericRecord])] =
      Hotswap(getWriter(zero))

    // save
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          persist[F, GenericRecord](
            getWriter,
            hotswap,
            writer,
            ss.map(Left(_)).mergeHaltBoth(tickStream[F](policy, zero).map(Right(_)))
          ).stream
        }
      }
  }
}

object HadoopJackson {
  def apply[F[_]](configuration: Configuration, schema: Schema): HadoopJackson[F] =
    new HadoopJackson[F](configuration, BLOCK_SIZE_HINT, CompressionLevel.DEFAULT_COMPRESSION, schema)
}
