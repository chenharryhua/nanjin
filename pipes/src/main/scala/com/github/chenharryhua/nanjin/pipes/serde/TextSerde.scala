package com.github.chenharryhua.nanjin.pipes.serde

import fs2.text.{lines, utf8}
import fs2.{Pipe, Stream}

object TextSerde {

  def serialize[F[_]]: Pipe[F, String, Byte] =
    (ss: Stream[F, String]) => ss.intersperse("\n").through(utf8.encode)

  def deserialize[F[_]]: Pipe[F, Byte, String] =
    (ss: Stream[F, Byte]) => ss.through(utf8.decode).through(lines)
}
