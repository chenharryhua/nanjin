package com.github.chenharryhua.nanjin

import cats.effect.kernel.Resource
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.chrono.Tick
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.numeric.Interval.Closed
import fs2.{Chunk, Pull, Stream}
import kantan.csv.CsvConfiguration
import kantan.csv.CsvConfiguration.Header
import kantan.csv.engine.WriterEngine

import java.io.StringWriter

package object terminals {
  @inline final val NEWLINE_SEPARATOR: String                = "\r\n"
  @inline private val NEWLINE_SEPARATOR_CHUNK: Chunk[String] = Chunk.singleton(NEWLINE_SEPARATOR)

  type NJCompressionLevel = Int Refined Closed[1, 9]
  object NJCompressionLevel extends RefinedTypeOps[NJCompressionLevel, Int] with CatsRefinedTypeOpsSyntax

  private[terminals] def persist[F[_], A](
    getWriter: Tick => Resource[F, HadoopWriter[F, A]],
    hotswap: Hotswap[F, HadoopWriter[F, A]],
    writer: HadoopWriter[F, A],
    ss: Stream[F, Either[Chunk[A], Tick]]
  ): Pull[F, Nothing, Unit] =
    ss.pull.uncons1.flatMap {
      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            Pull.eval(writer.write(data)) >> persist(getWriter, hotswap, writer, tail)
          case Right(tick) =>
            Pull.eval(hotswap.swap(getWriter(tick))).flatMap { writer =>
              persist(getWriter, hotswap, writer, tail)
            }
        }
      case None => Pull.done
    }

  private[terminals] def persistText[F[_]](
    getWriter: Tick => Resource[F, HadoopWriter[F, String]],
    hotswap: Hotswap[F, HadoopWriter[F, String]],
    writer: HadoopWriter[F, String],
    ss: Stream[F, Either[Chunk[String], Tick]],
    newLineSeparator: Chunk[String]
  ): Pull[F, Nothing, Unit] =
    ss.pull.uncons1.flatMap {
      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            val (rest, last) = data.splitAt(data.size - 1)
            Pull.eval(writer.write(newLineSeparator ++ rest.map(_.concat(NEWLINE_SEPARATOR)) ++ last)) >>
              persistText[F](getWriter, hotswap, writer, tail, NEWLINE_SEPARATOR_CHUNK)

          case Right(tick) =>
            Pull.eval(hotswap.swap(getWriter(tick))).flatMap { writer =>
              persistText(getWriter, hotswap, writer, tail, Chunk.empty)
            }
        }
      case None => Pull.done
    }

  private[terminals] def persistCsvWithHeader[F[_]](
    getWriter: Tick => Resource[F, HadoopWriter[F, String]],
    header: Chunk[String],
    hotswap: Hotswap[F, HadoopWriter[F, String]],
    writer: HadoopWriter[F, String],
    ss: Stream[F, Either[Chunk[String], Tick]],
    newLineSeparator: Chunk[String]
  ): Pull[F, Nothing, Unit] =
    ss.pull.uncons1.flatMap {
      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            val (rest, last) = data.splitAt(data.size - 1)
            Pull.eval(writer.write(newLineSeparator ++ rest.map(_.concat(NEWLINE_SEPARATOR)) ++ last)) >>
              persistCsvWithHeader[F](getWriter, header, hotswap, writer, tail, NEWLINE_SEPARATOR_CHUNK)

          case Right(tick) =>
            Pull.eval(hotswap.swap(getWriter(tick))).flatMap { writer =>
              Pull.eval(writer.write(header)) >>
                persistCsvWithHeader(getWriter, header, hotswap, writer, tail, NEWLINE_SEPARATOR_CHUNK)
            }
        }
      case None => Pull.done
    }

  def buildCsvRow(csvConfiguration: CsvConfiguration)(row: Seq[String]): String = {
    val sw = new StringWriter()
    WriterEngine.internalCsvWriterEngine.writerFor(sw, csvConfiguration).write(row).close()
    sw.toString.dropRight(2) // drop CRLF
  }

  def csvHeader(csvConfiguration: CsvConfiguration): Chunk[String] =
    csvConfiguration.header match {
      case Header.None             => Chunk.empty
      case Header.Implicit         => Chunk.singleton("no header was explicitly provided")
      case Header.Explicit(header) => Chunk.singleton(buildCsvRow(csvConfiguration)(header))
    }
}
