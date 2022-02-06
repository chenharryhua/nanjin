package com.github.chenharryhua.nanjin.pipes.serde
import fs2.text.{lines, utf8}
import fs2.{Pipe, RaiseThrowable, Stream}
import io.circe.parser.decode
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder, Json}

object CirceSerde {

  def serPipe[F[_], A](isKeepNull: Boolean)(implicit enc: JsonEncoder[A]): Pipe[F, A, Byte] = {
    def encode(a: A): Json = if (isKeepNull) enc(a) else enc(a).deepDropNullValues
    (ss: Stream[F, A]) => ss.mapChunks(_.map(encode(_).noSpaces)).intersperse(SEPERATOR).through(utf8.encode)
  }

  def deserPipe[F[_], A](implicit ev: RaiseThrowable[F], dec: JsonDecoder[A]): Pipe[F, Byte, A] =
    (ss: Stream[F, Byte]) =>
      ss.through(utf8.decode).through(lines).filter(_.trim.nonEmpty).mapChunks(_.map(decode[A])).rethrow

}
