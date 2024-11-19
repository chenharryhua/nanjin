package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Concurrent, Resource}
import cats.effect.std.Hotswap
import fs2.{Chunk, Pull, Stream}
import io.lemonlabs.uri.Url
private object periodically {

  /** @tparam F
    *   effect type
    * @tparam A
    *   type of data
    * @tparam R
    *   the getWriter's input type
    * @return
    */
  private def doWork[F[_], A, R](
    getWriter: R => Resource[F, HadoopWriter[F, A]],
    hotswap: Hotswap[F, HadoopWriter[F, A]],
    writer: HadoopWriter[F, A],
    merged: Stream[F, Either[Chunk[A], R]]
  ): Pull[F, Int, Unit] =
    merged.pull.uncons1.flatMap {
      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            Pull.output1(data.size) >>
              Pull.eval(writer.write(data)) >>
              doWork(getWriter, hotswap, writer, tail)
          case Right(tick) =>
            Pull.eval(hotswap.swap(getWriter(tick))).flatMap { writer =>
              doWork(getWriter, hotswap, writer, tail)
            }
        }
      case None => Pull.done
    }

  def persist[F[_]: Concurrent, A, R](
    data: Stream[F, Chunk[A]],
    ticks: Stream[F, R],
    getWriter: R => Resource[F, HadoopWriter[F, A]]): Stream[F, Int] =
    ticks.pull.uncons1.flatMap {
      case Some((head, tail)) => // use the very first tick to build writer and hotswap
        Stream
          .resource(Hotswap(getWriter(head)))
          .flatMap { case (hotswap, writer) =>
            doWork[F, A, R](
              getWriter,
              hotswap,
              writer,
              data.map(Left(_)).mergeHaltBoth(tail.map(Right(_)))
            ).stream
          }
          .pull
          .echo
      case None => Pull.done
    }.stream

  def persistCsvWithHeader[F[_]](
    getWriter: Url => Resource[F, HadoopWriter[F, String]],
    hotswap: Hotswap[F, HadoopWriter[F, String]],
    writer: HadoopWriter[F, String],
    ss: Stream[F, Either[Chunk[String], Url]],
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
