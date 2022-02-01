package com.github.chenharryhua.nanjin.pipes.serde

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Framing}
import akka.util.ByteString
import fs2.text.{lines, utf8}
import fs2.{Pipe, Stream}

object TextSerde {

  def serPipe[F[_]]: Pipe[F, String, Byte] =
    (ss: Stream[F, String]) => ss.intersperse("\n").through(utf8.encode)

  def deserPipe[F[_]]: Pipe[F, Byte, String] =
    (ss: Stream[F, Byte]) => ss.through(utf8.decode).through(lines)

  def serFlow: Flow[String, ByteString, NotUsed] =
    Flow[String].map(ByteString.fromString).intersperse(ByteString("\r\n"))

  def deserFlow: Flow[ByteString, String, NotUsed] =
    Flow[ByteString].via(Framing.delimiter(ByteString("\r\n"), Int.MaxValue, allowTruncation = true)).map(_.utf8String)
}
