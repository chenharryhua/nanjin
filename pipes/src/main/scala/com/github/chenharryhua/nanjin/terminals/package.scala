package com.github.chenharryhua.nanjin

import cats.effect.kernel.Resource
import cats.effect.std.Hotswap
import cats.implicits.toFoldableOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.numeric.Interval.Closed
import fs2.{Pull, Stream}
import squants.information.{Bytes, Information}

import java.nio.charset.StandardCharsets
package object terminals {
  final val NEWLINE_SEPERATOR: String            = "\r\n"
  final val NEWLINE_BYTES_SEPERATOR: Array[Byte] = NEWLINE_SEPERATOR.getBytes(StandardCharsets.UTF_8)

  final val BLOCK_SIZE_HINT: Long    = -1
  final val BUFFER_SIZE: Information = Bytes(8192)
  final val CHUNK_SIZE: ChunkSize    = ChunkSize(1000)

  type NJCompressionLevel = Int Refined Closed[1, 9]
  object NJCompressionLevel extends RefinedTypeOps[NJCompressionLevel, Int] with CatsRefinedTypeOpsSyntax

  private[terminals] def persist[F[_], A](
    writer: HadoopWriter[F, A],
    ss: Stream[F, A]): Pull[F, Nothing, Unit] =
    ss.pull.uncons.flatMap {
      case Some((hl, tl)) => Pull.eval(writer.write(hl)) >> persist(writer, tl)
      case None           => Pull.done
    }

  private[terminals] def rotatePersist[F[_], A](
    getWriter: Tick => Resource[F, HadoopWriter[F, A]],
    hotswap: Hotswap[F, HadoopWriter[F, A]],
    writer: HadoopWriter[F, A],
    ss: Stream[F, Either[A, Tick]]
  ): Pull[F, Nothing, Unit] =
    ss.pull.uncons.flatMap {
      case Some((head, tail)) =>
        val (data, ticks) = head.partitionEither(identity)
        ticks.last match {
          case Some(tick) =>
            Pull.eval(hotswap.swap(getWriter(tick))).flatMap { writer =>
              Pull.eval(writer.write(data)) >> rotatePersist(getWriter, hotswap, writer, tail)
            }
          case None =>
            Pull.eval(writer.write(data)) >> rotatePersist(getWriter, hotswap, writer, tail)
        }
      case None => Pull.done
    }
}
