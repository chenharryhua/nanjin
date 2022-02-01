package com.github.chenharryhua.nanjin.pipes.serde

import fs2.text.{lines, utf8}
import fs2.{Pipe, Stream}

final class TextSerde[F[_]] extends Serializable {

  def serialize: Pipe[F, String, Byte] =
    (ss: Stream[F, String]) => ss.intersperse("\n").through(utf8.encode)

  def deserialize: Pipe[F, Byte, String] =
    (ss: Stream[F, Byte]) => ss.through(utf8.decode).through(lines)
}
