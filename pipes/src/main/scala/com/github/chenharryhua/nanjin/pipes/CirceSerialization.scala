package com.github.chenharryhua.nanjin.pipes

import fs2.text.{lines, utf8Decode, utf8Encode}
import fs2.{Pipe, RaiseThrowable, Stream}
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}

final class CirceSerialization[F[_], A: JsonEncoder] {

  def serialize: Pipe[F, A, Byte] =
    (ss: Stream[F, A]) => ss.map(_.asJson.noSpaces).intersperse("\n").through(utf8Encode)

}

final class CirceDeserialization[F[_]: RaiseThrowable, A: JsonDecoder] {

  def deserialize: Pipe[F, Byte, A] =
    (ss: Stream[F, Byte]) => ss.through(utf8Decode).through(lines).map(decode[A]).rethrow

}
