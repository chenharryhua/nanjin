package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickStatus}
import fs2.text.utf8
import fs2.{Chunk, Pipe, Stream}
import kantan.csv.CsvConfiguration.Header
import kantan.csv.engine.ReaderEngine
import kantan.csv.{CsvConfiguration, CsvReader, ReadResult}
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
  final def modify(f: Endo[String]): Header.Explicit =
    header.focus(_.header).modify(_.map(f))
}

object CsvHeaderOf {
  def apply[A](implicit ev: CsvHeaderOf[A]): ev.type = ev

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

  def source(path: NJPath, chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, ReadResult[Seq[String]]] =
    HadoopReader.inputStreamS[F](configuration, path.hadoopPath).flatMap { is =>
      val reader: CsvReader[ReadResult[Seq[String]]] = {
        val cr = ReaderEngine.internalCsvReaderEngine
          .readerFor(new BufferedReader(new InputStreamReader(is)), csvConfiguration)
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

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Seq[String], Nothing] = {
    (ss: Stream[F, Seq[String]]) =>
      Stream.resource(HadoopWriter.byteR[F](configuration, path.hadoopPath)).flatMap { w =>
        val src: Stream[F, String] =
          Stream.chunk(csvHeader(csvConfiguration)) ++
            ss.map(buildCsvRow(csvConfiguration))
        src.intersperse(NEWLINE_SEPARATOR).through(utf8.encode).chunks.foreach(w.write)
      }
  }

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Seq[String], Nothing] = {
    def get_writer(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](configuration, StandardCharsets.UTF_8, pathBuilder(tick).hadoopPath)

    // save
    (ss: Stream[F, Seq[String]]) =>
      Stream.eval(TickStatus.zeroth[F](policy, zoneId)).flatMap { zero =>
        val ticks: Stream[F, Either[Chunk[String], Tick]] = tickStream[F](zero).map(Right(_))

        Stream.resource(Hotswap(get_writer(zero.tick))).flatMap { case (hotswap, writer) =>
          if (csvConfiguration.hasHeader) {
            val header: Chunk[String] = csvHeader(csvConfiguration)
            val src: Stream[F, Either[Chunk[String], Tick]] =
              (Stream(header) ++ ss.map(buildCsvRow(csvConfiguration)).chunks).map(Left(_))

            persistCsvWithHeader[F](
              get_writer,
              header,
              hotswap,
              writer,
              src.mergeHaltBoth(ticks),
              Chunk.empty
            ).stream
          } else {
            val src: Stream[F, Either[Chunk[String], Tick]] =
              ss.map(buildCsvRow(csvConfiguration)).chunks.map(Left(_))

            persistText[F](
              get_writer,
              hotswap,
              writer,
              src.mergeHaltBoth(ticks),
              Chunk.empty
            ).stream
          }
        }
      }
  }
}

object HadoopKantan {
  def apply[F[_]](hadoopCfg: Configuration, csvCfg: CsvConfiguration): HadoopKantan[F] =
    new HadoopKantan[F](hadoopCfg, csvCfg)
}
