package com.github.chenharryhua.nanjin.pipes

import akka.stream.IOResult
import akka.stream.alpakka.ftp.FtpSettings
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import cats.effect.{Async, ContextShift}
import io.circe.Encoder
import io.circe.syntax._

final class AkkaFtpSink[F[_]: Async: ContextShift](settings: FtpSettings) {

  private def toPath(pathStr: String): Sink[ByteString, F[IOResult]] =
    Ftp.toPath(pathStr, settings).mapMaterializedValue(f => Async.fromFuture(Async[F].pure(f)))

  def json[A: Encoder](pathStr: String): Sink[A, F[IOResult]] =
    toPath(pathStr).contramap[A](a => ByteString(a.asJson.noSpaces + "\n"))

}
