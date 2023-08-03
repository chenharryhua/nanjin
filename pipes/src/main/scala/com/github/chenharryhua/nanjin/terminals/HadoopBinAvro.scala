package com.github.chenharryhua.nanjin.terminals
import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy

import java.io.{EOFException, InputStream}

final class HadoopBinAvro[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  compressLevel: CompressionLevel,
  schema: Schema
) {

  def withBlockSizeHint(bsh: Long): HadoopBinAvro[F] =
    new HadoopBinAvro[F](configuration, bsh, compressLevel, schema)

  def withCompressionLevel(cl: CompressionLevel): HadoopBinAvro[F] =
    new HadoopBinAvro[F](configuration, blockSizeHint, cl, schema)

  def source(path: NJPath)(implicit F: Async[F]): Stream[F, GenericRecord] =
    HadoopReader.inputStream[F](configuration, path.hadoopPath).flatMap { is =>
      val datumReader = new GenericDatumReader[GenericRecord](schema)
      val avroDecoder = DecoderFactory.get().binaryDecoder(is, null)
      def go(is: InputStream): Pull[F, GenericRecord, Option[InputStream]] =
        Pull
          .functionKInstance(F.delay(try Option(datumReader.read(null, avroDecoder))
          catch {
            case _: EOFException => None
          }))
          .flatMap {
            case Some(a) => Pull.output1(a) >> Pull.pure[F, Option[InputStream]](Some(is))
            case None    => Pull.pure(None)
          }
      Pull.loop(go)(is).stream
    }

  def source(paths: List[NJPath])(implicit F: Async[F]): Stream[F, GenericRecord] =
    paths.foldLeft(Stream.empty.covaryAll[F, GenericRecord]) { case (s, p) =>
      s ++ source(p)
    }

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, GenericRecord, Nothing] = {
    (ss: Stream[F, GenericRecord]) =>
      Stream
        .resource(
          HadoopWriter.binAvro[F](configuration, compressLevel, blockSizeHint, schema, path.hadoopPath))
        .flatMap(w => persist[F, GenericRecord](w, ss).stream)
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, GenericRecord, Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, GenericRecord]] =
      HadoopWriter
        .binAvro[F](configuration, compressLevel, blockSizeHint, schema, pathBuilder(tick).hadoopPath)

    def init(
      tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, GenericRecord]], HadoopWriter[F, GenericRecord])] =
      Hotswap(
        HadoopWriter
          .binAvro[F](configuration, compressLevel, blockSizeHint, schema, pathBuilder(tick).hadoopPath))

    // save
    (ss: Stream[F, GenericRecord]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          rotatePersist[F, GenericRecord](
            getWriter,
            hotswap,
            writer,
            ss.map(Left(_)).mergeHaltL(tickStream[F](policy, zero).map(Right(_)))
          ).stream
        }
      }
  }
}

object HadoopBinAvro {
  def apply[F[_]](configuration: Configuration, schema: Schema): HadoopBinAvro[F] =
    new HadoopBinAvro[F](configuration, BLOCK_SIZE_HINT, CompressionLevel.DEFAULT_COMPRESSION, schema)
}
