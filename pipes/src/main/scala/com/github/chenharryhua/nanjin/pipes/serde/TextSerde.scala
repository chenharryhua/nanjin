package com.github.chenharryhua.nanjin.pipes.serde

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Framing}
import akka.util.ByteString
import fs2.text.{lines, utf8}
import fs2.{Pipe, Stream}

object TextSerde {

  def toBytes[F[_]]: Pipe[F, String, Byte] =
    (ss: Stream[F, String]) => ss.intersperse(NEWLINE_SEPERATOR).through(utf8.encode)

  def fromBytes[F[_]]: Pipe[F, Byte, String] =
    (ss: Stream[F, Byte]) => ss.through(utf8.decode).through(lines)

  object akka {
    def toByteString: Flow[String, ByteString, NotUsed] =
      Flow[String].map(ByteString.fromString).intersperse(ByteString(NEWLINE_SEPERATOR))

    def fromByteString: Flow[ByteString, String, NotUsed] =
      Flow[ByteString]
        .via(Framing.delimiter(ByteString(NEWLINE_SEPERATOR), Int.MaxValue, allowTruncation = true))
        .map(_.utf8String)
  }
}
