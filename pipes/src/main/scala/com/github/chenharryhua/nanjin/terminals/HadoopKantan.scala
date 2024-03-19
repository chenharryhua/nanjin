package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.text.utf8
import fs2.{Chunk, Pipe, Stream}
import kantan.csv.*
import kantan.csv.CsvConfiguration.Header
import kantan.csv.engine.ReaderEngine
import monocle.syntax.all.*
import org.apache.hadoop.conf.Configuration
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.Keys
import shapeless.{HList, LabelledGeneric}

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.time.ZoneId
import scala.annotation.nowarn

sealed trait CsvHeaderOf[A] {
  def header: Header.Explicit
  final def modify(f: String => String): Header.Explicit =
    header.focus(_.header).modify(_.map(f))
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
  csvConfiguration: CsvConfiguration
) {

  // read

  def source(path: NJPath, chunkSize: ChunkSize)(implicit
    F: Sync[F],
    engine: ReaderEngine): Stream[F, ReadResult[Seq[String]]] =
    HadoopReader.inputStreamS[F](configuration, path.hadoopPath).flatMap { is =>
      val reader: CsvReader[ReadResult[Seq[String]]] = {
        val cr = engine.readerFor(new BufferedReader(new InputStreamReader(is)), csvConfiguration)
        if (csvConfiguration.hasHeader) cr.drop(1) else cr
      }

      Stream.fromBlockingIterator[F](reader.iterator, chunkSize.value)
    }

  def source(paths: List[NJPath], chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, ReadResult[Seq[String]]] =
    paths.foldLeft(Stream.empty.covaryAll[F, ReadResult[Seq[String]]]) { case (s, p) =>
      s ++ source(p, chunkSize)
    }

  // write

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[Seq[String]], Nothing] = {
    (ss: Stream[F, Chunk[Seq[String]]]) =>
      Stream.resource(HadoopWriter.byteR[F](configuration, path.hadoopPath)).flatMap { w =>
        val src: Stream[F, Chunk[String]] =
          Stream(csvHeader(csvConfiguration)).filter(_.nonEmpty) ++
            ss.map(_.map(buildCsvRow(csvConfiguration)))
        src.unchunks.intersperse(NEWLINE_SEPARATOR).through(utf8.encode).chunks.foreach(w.write)
      }
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[Seq[String]], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](configuration, StandardCharsets.UTF_8, pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, Chunk[Seq[String]]]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        Stream.resource(Hotswap(getWriter(zero.tick))).flatMap { case (hotswap, writer) =>
          val header: Chunk[String] = csvHeader(csvConfiguration)
          val src: Stream[F, Chunk[String]] =
            Stream(header).filter(_.nonEmpty) ++ ss.map(_.map(buildCsvRow(csvConfiguration)))

          val header_crlf = header.map(_.concat(NEWLINE_SEPARATOR))
          val ts: Stream[F, Either[Chunk[String], (Tick, Chunk[String])]] =
            tickStream[F](zero).map(t => Right((t, header_crlf)))

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
    new HadoopKantan[F](hadoopCfg, csvCfg)
}
