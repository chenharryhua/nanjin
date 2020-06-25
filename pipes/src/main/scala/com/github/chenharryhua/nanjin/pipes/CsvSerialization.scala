package com.github.chenharryhua.nanjin.pipes

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift}
import cats.implicits._
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Pull, Stream}
import kantan.csv.{CsvConfiguration, CsvWriter, RowDecoder, RowEncoder}

final class CsvSerialization[F[_]: Concurrent: ContextShift, A: RowEncoder](
  conf: CsvConfiguration,
  blocker: Blocker) {
  import kantan.csv.ops._

  def serialize: Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    for {
      bs <- readOutputStream[F](blocker, chunkSize) { os =>
        def go(as: Stream[F, A], cw: CsvWriter[A]): Pull[F, Unit, Unit] =
          as.pull.uncons.flatMap {
            case Some((hl, tl)) => Pull.pure(hl.foreach(cw.write)) >> go(tl, cw)
            case None           => Pull.pure(cw.close()) >> Pull.done
          }
        go(ss, os.asCsvWriter(conf)).stream.compile.drain
      }
    } yield bs
  }
}

final class CsvDeserialization[F[_]: ConcurrentEffect, A: RowDecoder](conf: CsvConfiguration) {
  import kantan.csv.ops._

  def deserialize: Pipe[F, Byte, A] = { (ss: Stream[F, Byte]) =>
    ss.through(toInputStream[F])
      .flatMap(is => Stream.fromIterator[F](is.asCsvReader[A](conf).toIterator).rethrow)
  }
}
