package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.{Pipe, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy

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

  def source(path: NJPath)(implicit F: Async[F]): Stream[F, GenericRecord] =
    HadoopReader.jacksonS[F](configuration, schema, path.hadoopPath)

  def source(paths: List[NJPath])(implicit F: Async[F]): Stream[F, GenericRecord] =
    paths.foldLeft(Stream.empty.covaryAll[F, GenericRecord]) { case (s, p) =>
      s ++ source(p)
    }

  // write

  private def getWriterR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, GenericRecord]] =
    HadoopWriter.jacksonR[F](configuration, compressLevel, blockSizeHint, schema, path)

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, GenericRecord, Nothing] = {
    (ss: Stream[F, GenericRecord]) =>
      Stream.resource(getWriterR(path.hadoopPath)).flatMap(w => ss.chunks.foreach(w.write))
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, GenericRecord, Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      getWriterR(pathBuilder(tick).hadoopPath)

    def init(
      zero: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, GenericRecord]], HadoopWriter[F, GenericRecord])] =
      Hotswap(getWriter(zero))

    // save
    (ss: Stream[F, GenericRecord]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          persist[F, GenericRecord](
            getWriter,
            hotswap,
            writer,
            ss.chunks.map(Left(_)).mergeHaltL(tickStream[F](policy, zero).map(Right(_)))
          ).stream
        }
      }
  }
}

object HadoopJackson {
  def apply[F[_]](configuration: Configuration, schema: Schema): HadoopJackson[F] =
    new HadoopJackson[F](configuration, BLOCK_SIZE_HINT, CompressionLevel.DEFAULT_COMPRESSION, schema)
}
