package com.github.chenharryhua.nanjin.pipes

import akka.stream.Materializer
import akka.stream.alpakka.ftp.{FtpSettings, SftpSettings}
import akka.stream.alpakka.ftp.scaladsl.{Ftp, Sftp}
import akka.util.ByteString
import cats.effect.{Async, Concurrent, ContextShift}
import cats.implicits._
import streamz.converter._
import fs2.Stream

trait FtpSource[F[_]] {
  def download(pathStr: String): Stream[F, ByteString]
}

final class AkkaFtpSource[F[_]: ContextShift: Concurrent](settings: FtpSettings)(implicit
  mat: Materializer)
    extends FtpSource[F] {

  override def download(pathStr: String): Stream[F, ByteString] = {
    val run = Ftp.fromPath(pathStr, settings).toStreamMat[F].map {
      case (s, f) =>
        s.concurrently(Stream.eval(Async.fromFuture(Async[F].pure(f))))
    }
    Stream.eval(run).flatten
  }
}

final class AkkaSftpSource[F[_]: ContextShift: Concurrent](settings: SftpSettings)(implicit
  mat: Materializer)
    extends FtpSource[F] {

  override def download(pathStr: String): Stream[F, ByteString] = {
    val run = Sftp.fromPath(pathStr, settings).toStreamMat[F].map {
      case (s, f) =>
        s.concurrently(Stream.eval(Async.fromFuture(Async[F].pure(f))))
    }
    Stream.eval(run).flatten
  }
}
