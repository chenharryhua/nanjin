package com.github.chenharryhua.nanjin.pipes.serde
import fs2.text.{lines, utf8}
import fs2.{Pipe, RaiseThrowable, Stream}
import io.circe.parser.decode
import io.circe.{Json, Decoder as JsonDecoder, Encoder as JsonEncoder}

final class CirceSerialization[F[_], A] extends Serializable {

  def serialize(isKeepNull: Boolean)(implicit enc: JsonEncoder[A]): Pipe[F, A, Byte] = {
    def encode(a: A): Json = if (isKeepNull) enc(a) else enc(a).deepDropNullValues
    (ss: Stream[F, A]) => ss.map(encode(_).noSpaces).intersperse("\n").through(utf8.encode)
  }

  def deserialize(implicit ev: RaiseThrowable[F], dec: JsonDecoder[A]): Pipe[F, Byte, A] =
    (ss: Stream[F, Byte]) => ss.through(utf8.decode).through(lines).filter(_.trim.nonEmpty).map(decode[A]).rethrow

}
