package com.github.chenharryhua.nanjin.pipes

import fs2.{Pipe, Stream}
import fs2.text.{lines, utf8Decode, utf8Encode}

final class TextSerialization[F[_]] extends Serializable {

  def serialize: Pipe[F, String, Byte] =
    (ss: Stream[F, String]) => ss.intersperse("\n").through(utf8Encode)

  def deserialize: Pipe[F, Byte, String] =
    (ss: Stream[F, Byte]) => ss.through(utf8Decode).through(lines)
}
