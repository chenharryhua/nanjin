package com.github.chenharryhua.nanjin.pipes.serde

import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Pull, Stream}
import kantan.csv.{CsvConfiguration, CsvWriter, HeaderDecoder, HeaderEncoder}
import squants.information.Information

object CsvSerde {
  import kantan.csv.ops.*

  def serPipe[F[_], A](conf: CsvConfiguration, byteBuffer: Information)(implicit
    enc: HeaderEncoder[A],
    F: Async[F]): Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    readOutputStream[F](byteBuffer.toBytes.toInt) { os =>
      def go(as: Stream[F, A], cw: CsvWriter[A]): Pull[F, Unit, Unit] =
        as.pull.uncons.flatMap {
          case Some((hl, tl)) =>
            Pull.eval(F.delay(hl.foldLeft(cw) { case (w, item) => w.write(item) })).flatMap(go(tl, _))
          case None => Pull.eval(F.blocking(cw.close())) >> Pull.done
        }
      go(ss, os.asCsvWriter[A](conf)).stream.compile.drain
    }
  }

  def deserPipe[F[_], A](conf: CsvConfiguration, chunkSize: ChunkSize)(implicit
    dec: HeaderDecoder[A],
    F: Async[F]): Pipe[F, Byte, A] =
    _.through(toInputStream[F]).flatMap(is =>
      Stream.fromBlockingIterator[F](is.asCsvReader[A](conf).iterator, chunkSize.value).rethrow)
}
