package com.github.chenharryhua.nanjin.pipes.serde

import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.pipes.chunkSize
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Pull, Stream}
import kantan.csv.{CsvConfiguration, CsvWriter, RowDecoder, RowEncoder}

final class CsvSerialization[F[_], A](conf: CsvConfiguration) extends Serializable {
  import kantan.csv.ops.*

  def serialize(implicit enc: RowEncoder[A], F: Async[F]): Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    readOutputStream[F](chunkSize) { os =>
      def go(as: Stream[F, A], cw: CsvWriter[A]): Pull[F, Unit, Unit] =
        as.pull.uncons.flatMap {
          case Some((hl, tl)) =>
            Pull.eval(F.blocking(hl.foldLeft(cw) { case (w, item) => w.write(item) })).flatMap(go(tl, _))
          case None => Pull.eval(F.blocking(cw.close())) >> Pull.done
        }
      go(ss, os.asCsvWriter(conf)).stream.compile.drain
    }
  }

  def deserialize(implicit dec: RowDecoder[A], F: Async[F]): Pipe[F, Byte, A] =
    _.through(toInputStream[F]).flatMap(is =>
      Stream.fromIterator[F](is.asCsvReader[A](conf).toIterator, chunkSize).rethrow)
}
