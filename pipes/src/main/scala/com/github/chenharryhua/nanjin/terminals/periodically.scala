package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Concurrent, Resource}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickedValue}
import fs2.{Chunk, Pull, Stream}
private object periodically {
  object merge {

    /** @tparam F
      *   effect type
      * @tparam A
      *   type of data
      * @tparam R
      *   the getWriter's input type
      * @return
      */
    private def doWork[F[_], A, R](
      currentTick: Tick,
      getWriter: R => Resource[F, HadoopWriter[F, A]],
      hotswap: Hotswap[F, HadoopWriter[F, A]],
      writer: HadoopWriter[F, A],
      merged: Stream[F, Either[Chunk[A], TickedValue[R]]]
    ): Pull[F, TickedValue[Int], Unit] =
      merged.pull.uncons1.flatMap {
        case Some((head, tail)) =>
          head match {
            case Left(data) =>
              Pull.output1(TickedValue(currentTick, data.size)) >>
                Pull.eval(writer.write(data)) >>
                doWork(currentTick, getWriter, hotswap, writer, tail)
            case Right(ticked) =>
              Pull.eval(hotswap.swap(getWriter(ticked.value))).flatMap { writer =>
                doWork(ticked.tick, getWriter, hotswap, writer, tail)
              }
          }
        case None => Pull.done
      }

    def persist[F[_]: Concurrent, A, R](
      data: Stream[F, Chunk[A]],
      ticks: Stream[F, TickedValue[R]],
      getWriter: R => Resource[F, HadoopWriter[F, A]]): Pull[F, TickedValue[Int], Unit] =
      ticks.pull.uncons1.flatMap {
        case Some((head, tail)) => // use the very first tick to build writer and hotswap
          Stream
            .resource(Hotswap(getWriter(head.value)))
            .flatMap { case (hotswap, writer) =>
              doWork[F, A, R](
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

  object zip {
    def persist[F[_]: Concurrent, A, R](
      getWriter: R => Resource[F, HadoopWriter[F, A]],
      zipped: Stream[F, (Chunk[A], TickedValue[R])]
    ): Pull[F, TickedValue[Int], Unit] =
      zipped.pull.uncons1.flatMap {
        case Some(((data, ticked), tail)) =>
          Pull.eval(getWriter(ticked.value).use(_.write(data))) >>
            Pull.output1(TickedValue(ticked.tick, data.size)) >>
            persist(getWriter, tail)
        case None => Pull.done
      }
  }

  def persistCsvWithHeader[F[_], R](
    currentTick: Tick,
    getWriter: R => Resource[F, HadoopWriter[F, String]],
    hotswap: Hotswap[F, HadoopWriter[F, String]],
    writer: HadoopWriter[F, String],
    ss: Stream[F, Either[Chunk[String], TickedValue[R]]],
    header: Chunk[String]
  ): Pull[F, TickedValue[Int], Unit] =
    ss.pull.uncons1.flatMap {
      case Some((head, tail)) =>
        head match {
          case Left(data) =>
            Pull.output1(TickedValue(currentTick, data.size)) >>
              Pull.eval(writer.write(data)) >>
              persistCsvWithHeader[F, R](currentTick, getWriter, hotswap, writer, tail, header)

          case Right(ticked) =>
            Pull.eval(hotswap.swap(getWriter(ticked.value))).flatMap { writer =>
              Pull.eval(writer.write(header)) >>
                persistCsvWithHeader(ticked.tick, getWriter, hotswap, writer, tail, header)
            }
        }
      case None => Pull.done
    }
}
