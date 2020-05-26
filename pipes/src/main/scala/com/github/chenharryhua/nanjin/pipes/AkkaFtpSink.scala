package com.github.chenharryhua.nanjin.pipes

import akka.stream.alpakka.ftp.{FtpSettings, SftpSettings}
import akka.stream.alpakka.ftp.scaladsl.{Ftp, Sftp}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import cats.effect.{ConcurrentEffect, ContextShift}
import cats.implicits._
import com.sksamuel.avro4s.{SchemaFor, Encoder => AvroEncoder}
import fs2.{Pipe, Stream}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import streamz.converter._

trait FtpSink[F[_]] {
  def upload(pathStr: String): Pipe[F, ByteString, IOResult]

  final def json[A: JsonEncoder](pathStr: String): Pipe[F, A, IOResult] =
    upload(pathStr).compose(jsonEncode[F, A].andThen(_.intersperse("\n").map(ByteString(_))))

  final def jackson[A: AvroEncoder: SchemaFor](pathStr: String): Pipe[F, A, IOResult] =
    upload(pathStr).compose(jacksonEncode[F, A].andThen(_.intersperse("\n").map(ByteString(_))))

  final def csv[A: HeaderEncoder](pathStr: String, conf: CsvConfiguration): Pipe[F, A, IOResult] =
    upload(pathStr).compose(csvEncode[F, A](conf).andThen(_.intersperse("\n").map(ByteString(_))))

  final def csv[A: HeaderEncoder](pathStr: String): Pipe[F, A, IOResult] =
    csv[A](pathStr, CsvConfiguration.rfc)
}

final class AkkaFtpSink[F[_]: ConcurrentEffect: ContextShift](settings: FtpSettings)(implicit
  mat: Materializer)
    extends FtpSink[F] {
  import mat.executionContext

  override def upload(pathStr: String): Pipe[F, ByteString, IOResult] = {
    (ss: Stream[F, ByteString]) =>
      Stream
        .eval(Ftp.toPath(pathStr, settings).toPipeMatWithResult[F])
        .flatMap(p => ss.through(p).rethrow)
  }
}

final class AkkaSftpSink[F[_]: ConcurrentEffect: ContextShift](settings: SftpSettings)(implicit
  mat: Materializer)
    extends FtpSink[F] {
  import mat.executionContext

  override def upload(pathStr: String): Pipe[F, ByteString, IOResult] = {
    (ss: Stream[F, ByteString]) =>
      Stream
        .eval(Sftp.toPath(pathStr, settings).toPipeMatWithResult[F])
        .flatMap(p => ss.through(p).rethrow)
  }
}
