package com.github.chenharryhua.nanjin

import cats.effect.kernel.Resource
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.numeric.Interval.Closed
import fs2.{Chunk, Pull, Stream}
import squants.information.{Bytes, Information}
package object terminals {
  @inline final val NEWLINE_SEPARATOR: String                = "\r\n"
  @inline private val NEWLINE_SEPARATOR_CHUNK: Chunk[String] = Chunk(NEWLINE_SEPARATOR)

  final val BLOCK_SIZE_HINT: Long    = -1
  final val BUFFER_SIZE: Information = Bytes(8192)
  final val CHUNK_SIZE: ChunkSize    = ChunkSize(1000)

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

  private[terminals] def persistString[F[_]](
    getWriter: Tick => Resource[F, HadoopWriter[F, String]],
    hotswap: Hotswap[F, HadoopWriter[F, String]],
    writer: HadoopWriter[F, String],
    ss: Stream[F, Either[Chunk[String], Tick]],
    newLineSeparator: Chunk[String],
    header: Chunk[String] // used by csv, text and circe will set it to empty
  ): Pull[F, Nothing, Unit] =
    ss.pull.uncons1.flatMap {
      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            val (rest, last) = data.splitAt(data.size - 1)
            Pull.eval(writer.write(newLineSeparator ++ rest.map(_.concat(NEWLINE_SEPARATOR)) ++ last)) >>
              persistString[F](getWriter, hotswap, writer, tail, NEWLINE_SEPARATOR_CHUNK, header)

          case Right(tick) =>
            Pull.eval(hotswap.swap(getWriter(tick))).flatMap { writer =>
              Pull.eval(writer.write(header)) >>
                persistString(getWriter, hotswap, writer, tail, Chunk.empty, header)
            }
        }
      case None => Pull.done
    }
}
