package com.github.chenharryhua.nanjin.pipes

import fs2.text.{lines, utf8Decode, utf8Encode}
import fs2.{Pipe, RaiseThrowable, Stream}
import io.circe.parser.decode
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}

final class CirceSerialization[F[_], A](enc: JsonEncoder[A]) {

  def serialize: Pipe[F, A, Byte] =
    (ss: Stream[F, A]) => ss.map(enc(_).noSpaces).intersperse("\n").through(utf8Encode)

}

final class CirceDeserialization[F[_]: RaiseThrowable, A](dec: JsonDecoder[A]) {
  implicit private val decoder: JsonDecoder[A] = dec

  def deserialize: Pipe[F, Byte, A] =
    (ss: Stream[F, Byte]) => ss.through(utf8Decode).through(lines).map(decode[A]).rethrow

}
