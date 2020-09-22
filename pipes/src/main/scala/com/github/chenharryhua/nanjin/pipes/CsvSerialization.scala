package com.github.chenharryhua.nanjin.pipes

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift}
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Pull, Stream}
import kantan.csv.{CsvConfiguration, CsvWriter, RowDecoder, RowEncoder}

final class CsvSerialization[F[_], A](conf: CsvConfiguration) extends Serializable {
  import kantan.csv.ops._

  def serialize(blocker: Blocker)(implicit
    enc: RowEncoder[A],
    cc: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    readOutputStream[F](blocker, 8096) { os =>
      def go(as: Stream[F, A], cw: CsvWriter[A]): Pull[F, Unit, Unit] =
        as.pull.uncons.flatMap {
          case Some((hl, tl)) =>
            Pull.pure(hl.foldLeft(cw) { case (w, item) => w.write(item) }).flatMap(go(tl, _))
          case None => Pull.pure(cw.close()) >> Pull.done
        }
      go(ss, os.asCsvWriter(conf)).stream.compile.drain
    }
  }

  def deserialize(implicit dec: RowDecoder[A], ce: ConcurrentEffect[F]): Pipe[F, Byte, A] =
    _.through(toInputStream[F]).flatMap(is =>
      Stream.fromIterator[F](is.asCsvReader[A](conf).toIterator).rethrow)
}
