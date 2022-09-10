package com.github.chenharryhua.nanjin.pipes

import com.github.chenharryhua.nanjin.terminals.NEWLINE_SEPERATOR
import fs2.{Pipe, Stream}
import fs2.text.{lines, utf8}

object TextSerde {

  def toBytes[F[_]]: Pipe[F, String, Byte] =
    (ss: Stream[F, String]) => ss.intersperse(NEWLINE_SEPERATOR).through(utf8.encode)

  def fromBytes[F[_]]: Pipe[F, Byte, String] =
    (ss: Stream[F, Byte]) => ss.through(utf8.decode).through(lines)

}
