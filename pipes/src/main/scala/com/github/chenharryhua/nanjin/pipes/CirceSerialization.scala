package com.github.chenharryhua.nanjin.pipes

import fs2.text.{lines, utf8Decode, utf8Encode}
import fs2.{Pipe, RaiseThrowable, Stream}
import io.circe.parser.decode
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}

final class CirceSerialization[F[_], A] extends Serializable {

  def serialize(implicit enc: JsonEncoder[A]): Pipe[F, A, Byte] =
    (ss: Stream[F, A]) => ss.map(enc(_).noSpaces).intersperse("\n").through(utf8Encode)

  def deserialize(implicit ev: RaiseThrowable[F], dec: JsonDecoder[A]): Pipe[F, Byte, A] =
    (ss: Stream[F, Byte]) => ss.through(utf8Decode).through(lines).map(decode[A]).rethrow

}
