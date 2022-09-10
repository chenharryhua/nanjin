package com.github.chenharryhua.nanjin.pipes

import com.github.chenharryhua.nanjin.terminals.NEWLINE_SEPERATOR
import fs2.{Pipe, RaiseThrowable, Stream}
import fs2.text.{lines, utf8}
import io.circe.{Json, Decoder as JsonDecoder, Encoder as JsonEncoder}
import io.circe.parser.decode

object CirceSerde {

  @inline private def encoder[A](isKeepNull: Boolean, enc: JsonEncoder[A]): A => Json =
    (a: A) => if (isKeepNull) enc(a) else enc(a).deepDropNullValues

  def toBytes[F[_], A](isKeepNull: Boolean)(implicit enc: JsonEncoder[A]): Pipe[F, A, Byte] = {
    val encode = encoder[A](isKeepNull, enc)
    (ss: Stream[F, A]) =>
      ss.mapChunks(_.map(encode(_).noSpaces)).intersperse(NEWLINE_SEPERATOR).through(utf8.encode)
  }

  def fromBytes[F[_], A](implicit ev: RaiseThrowable[F], dec: JsonDecoder[A]): Pipe[F, Byte, A] =
    (ss: Stream[F, Byte]) =>
      ss.through(utf8.decode).through(lines).filter(_.nonEmpty).mapChunks(_.map(decode[A])).rethrow

}
