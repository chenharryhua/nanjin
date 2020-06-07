package com.github.chenharryhua.nanjin.pipes

import fs2.{Pipe, Pure, RaiseThrowable, Stream}
import kantan.csv.{CsvConfiguration, HeaderEncoder, RowDecoder}
import fs2.text.{lines, utf8Decode, utf8Encode}

final class CsvSerialization[F[_], A: HeaderEncoder](conf: CsvConfiguration) {

  def serialize: Pipe[F, A, Byte] =
    (ss: Stream[F, A]) => {
      val hs: Stream[Pure, String] = conf.header match {
        case CsvConfiguration.Header.Implicit =>
          HeaderEncoder[A].header match {
            case Some(h) => Stream(h.mkString(conf.cellSeparator.toString))
            case None    => Stream("")
          }
        case CsvConfiguration.Header.Explicit(seq) =>
          Stream(seq.mkString(conf.cellSeparator.toString))
        case CsvConfiguration.Header.None => Stream()
      }
      (hs.covary[F] ++ ss.map(m =>
        HeaderEncoder[A].rowEncoder.encode(m).mkString(conf.cellSeparator.toString)))
        .intersperse("\n")
        .through(utf8Encode[F])
    }
}

final class CsvDeserialization[F[_]: RaiseThrowable, A: RowDecoder](conf: CsvConfiguration) {

  def deserialize: Pipe[F, Byte, A] =
    _.through(utf8Decode)
      .through(lines)
      .map(r => RowDecoder[A].decode(r.split(conf.cellSeparator)))
      .rethrow

}
