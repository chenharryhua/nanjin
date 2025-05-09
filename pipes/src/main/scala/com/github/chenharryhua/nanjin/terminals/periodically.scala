package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Concurrent, Resource}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import fs2.{Chunk, Pull, Stream}
import org.apache.hadoop.fs.Path
private object periodically {

  /** @tparam F
    *   effect type
    * @tparam A
    *   type of data
    * @return
    */
  private def doWork[F[_], A](
    currentTick: Tick,
    getWriter: Path => Resource[F, HadoopWriter[F, A]],
    hotswap: Hotswap[F, HadoopWriter[F, A]],
    writer: HadoopWriter[F, A],
    merged: Stream[F, Either[Chunk[A], TickedValue[Path]]]
  ): Pull[F, TickedValue[Int], Unit] =
    merged.pull.uncons1.flatMap {
      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            Pull.eval(writer.write(data)) >>
              Pull.output1(TickedValue(currentTick, data.size)) >>
              doWork(currentTick, getWriter, hotswap, writer, tail)
          case Right(ticked) =>
            Pull.eval(hotswap.swap(getWriter(ticked.value))).flatMap { writer =>
              doWork(ticked.tick, getWriter, hotswap, writer, tail)
            }
        }
      case None => Pull.done
    }

  def persist[F[_]: Concurrent, A](
    data: Stream[F, Chunk[A]],
    ticks: Stream[F, TickedValue[Path]],
    getWriter: Path => Resource[F, HadoopWriter[F, A]]): Pull[F, TickedValue[Int], Unit] =
    ticks.pull.uncons1.flatMap {
      case Some((head, tail)) => // use the very first tick to build writer and hotswap
        Stream
          .resource(Hotswap(getWriter(head.value)))
          .flatMap { case (hotswap, writer) =>
            doWork[F, A](
              head.tick,
              getWriter,
              hotswap,
              writer,
              data.map(Left(_)).mergeHaltBoth(tail.map(Right(_)))
            ).stream
          }
          .pull
          .echo
      case None => Pull.done
    }
}
