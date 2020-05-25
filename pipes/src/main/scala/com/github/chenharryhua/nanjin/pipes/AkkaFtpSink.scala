package com.github.chenharryhua.nanjin.pipes

import akka.stream.alpakka.ftp.FtpSettings
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import cats.effect.{ConcurrentEffect, ContextShift}
import fs2.Pipe
import io.circe.Encoder
import io.circe.syntax._
import streamz.converter._

final class AkkaFtpSink[F[_]: ConcurrentEffect: ContextShift](settings: FtpSettings)(implicit
  materializer: Materializer) {
  import materializer.executionContext

  def json[A: Encoder](pathStr: String): Pipe[F, A, IOResult] =
    Ftp
      .toPath(pathStr, settings)
      .contramap[A](a => ByteString(a.asJson.noSpaces + "\n"))
      .toPipeMat[F]
      .andThen(_.rethrow)

}
