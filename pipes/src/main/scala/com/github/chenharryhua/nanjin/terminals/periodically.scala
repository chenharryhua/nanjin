package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Resource
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.chrono.Tick
import fs2.{Chunk, Pull, Stream}

private object periodically {

  def persist[F[_], A](
    getWriter: Tick => Resource[F, HadoopWriter[F, A]],
    hotswap: Hotswap[F, HadoopWriter[F, A]],
    writer: HadoopWriter[F, A],
    ss: Stream[F, Either[Chunk[A], Tick]]
  ): Pull[F, Int, Unit] =
    ss.pull.uncons1.flatMap {
      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            Pull.output1(data.size) >>
              Pull.eval(writer.write(data)) >>
              persist(getWriter, hotswap, writer, tail)
          case Right(tick) =>
            Pull.eval(hotswap.swap(getWriter(tick))).flatMap { writer =>
              persist(getWriter, hotswap, writer, tail)
            }
        }
      case None => Pull.done
    }

  def persistCsvWithHeader[F[_]](
    getWriter: Tick => Resource[F, HadoopWriter[F, String]],
    hotswap: Hotswap[F, HadoopWriter[F, String]],
    writer: HadoopWriter[F, String],
    ss: Stream[F, Either[Chunk[String], Tick]],
    header: Chunk[String]
  ): Pull[F, Int, Unit] =
    ss.pull.uncons1.flatMap {
      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            Pull.output1(data.size) >>
              Pull.eval(writer.write(data)) >>
              persistCsvWithHeader[F](getWriter, hotswap, writer, tail, header)

          case Right(tick) =>
            Pull.eval(hotswap.swap(getWriter(tick))).flatMap { writer =>
              Pull.eval(writer.write(header)) >>
                persistCsvWithHeader(getWriter, hotswap, writer, tail, header)
            }
        }
      case None => Pull.done
    }

}
