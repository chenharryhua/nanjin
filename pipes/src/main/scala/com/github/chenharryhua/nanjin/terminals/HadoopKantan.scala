package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.policies.Policy
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.text.utf8
import fs2.{Chunk, Pipe, Stream}
import kantan.csv.*
import kantan.csv.CsvConfiguration.Header
import kantan.csv.engine.ReaderEngine
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.Keys
import shapeless.{HList, LabelledGeneric}

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import scala.annotation.nowarn

sealed trait CsvHeaderOf[A] {
  def header: Header.Explicit
}

object CsvHeaderOf {
  def apply[A](implicit ev: CsvHeaderOf[A]): Header.Explicit = ev.header

  implicit def inferKantanCsvHeader[A, Repr <: HList, KeysRepr <: HList](implicit
    @nowarn gen: LabelledGeneric.Aux[A, Repr],
    keys: Keys.Aux[Repr, KeysRepr],
    traversable: ToTraversable.Aux[KeysRepr, List, Symbol]): CsvHeaderOf[A] =
    new CsvHeaderOf[A] {
      override val header: Header.Explicit = Header.Explicit(keys().toList.map(_.name))
    }
}

final class HadoopKantan[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  compressLevel: CompressionLevel,
  csvConfiguration: CsvConfiguration
) {

  // config

  def withBlockSizeHint(bsh: Long): HadoopKantan[F] =
    new HadoopKantan[F](configuration, bsh, compressLevel, csvConfiguration)

  def withCompressionLevel(cl: CompressionLevel): HadoopKantan[F] =
    new HadoopKantan[F](configuration, blockSizeHint, cl, csvConfiguration)

  // read

  def source(path: NJPath, chunkSize: ChunkSize)(implicit
    F: Sync[F],
    engine: ReaderEngine): Stream[F, Seq[String]] =
    HadoopReader.inputStreamS[F](configuration, path.hadoopPath).flatMap { is =>
      val reader: CsvReader[Seq[String]] =
        if (csvConfiguration.hasHeader)
          engine.unsafeReaderFor(new InputStreamReader(is), csvConfiguration).drop(1)
        else
          engine.unsafeReaderFor(new InputStreamReader(is), csvConfiguration)

      Stream.fromBlockingIterator[F](reader.iterator, chunkSize.value)
    }

  def source(paths: List[NJPath], chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, Seq[String]] =
    paths.foldLeft(Stream.empty.covaryAll[F, Seq[String]]) { case (s, p) => s ++ source(p, chunkSize) }

  // write

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[Seq[String]], Nothing] = {
    (ss: Stream[F, Chunk[Seq[String]]]) =>
      Stream
        .resource(HadoopWriter.byteR[F](configuration, compressLevel, blockSizeHint, path.hadoopPath))
        .flatMap { w =>
          val src: Stream[F, Chunk[String]] =
            Stream(csvHeader(csvConfiguration)).filter(_.nonEmpty) ++
              ss.map(_.map(buildCsvRow(csvConfiguration)))
          src.unchunks.intersperse(NEWLINE_SEPARATOR).through(utf8.encode).chunks.foreach(w.write)
        }
  }

  def sink(policy: Policy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[Seq[String]], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](
        configuration,
        compressLevel,
        blockSizeHint,
        StandardCharsets.UTF_8,
        pathBuilder(tick).hadoopPath)

    def init(tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, String]], HadoopWriter[F, String])] =
      Hotswap(getWriter(tick))

    // save
    (ss: Stream[F, Chunk[Seq[String]]]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          val header: Chunk[String] = csvHeader(csvConfiguration)
          val src: Stream[F, Chunk[String]] =
            Stream(header).filter(_.nonEmpty) ++ ss.map(_.map(buildCsvRow(csvConfiguration)))

          val header_crlf = header.map(_.concat(NEWLINE_SEPARATOR))
          val ts: Stream[F, Either[Chunk[String], (Tick, Chunk[String])]] =
            tickStream[F](policy, zero).map(t => Right((t, header_crlf)))

          persistString[F](
            getWriter,
            hotswap,
            writer,
            src.map(Left(_)).mergeHaltBoth(ts),
            Chunk.empty
          ).stream
        }
      }
  }
}

object HadoopKantan {
  def apply[F[_]](hadoopCfg: Configuration, csvCfg: CsvConfiguration): HadoopKantan[F] =
    new HadoopKantan[F](hadoopCfg, BLOCK_SIZE_HINT, CompressionLevel.DEFAULT_COMPRESSION, csvCfg)
}
