package com.github.chenharryhua.nanjin.pipes

import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import cats.effect.{ConcurrentEffect, ContextShift}
import cats.implicits._
import com.sksamuel.avro4s.{SchemaFor, Encoder => AvroEncoder}
import fs2.{Pipe, Stream}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}
import streamz.converter._

sealed class FtpSink[F[_]: ConcurrentEffect: ContextShift, C, S <: RemoteFileSettings](
  ftpApi: FtpApi[C, S],
  settings: S)(implicit mat: Materializer) {

  import mat.executionContext

  final def upload(pathStr: String): Pipe[F, ByteString, IOResult] = {
    (ss: Stream[F, ByteString]) =>
      Stream
        .eval(ftpApi.toPath(pathStr, settings).toPipeMatWithResult[F])
        .flatMap(p => ss.through(p).rethrow)
  }

  final def json[A: JsonEncoder](pathStr: String): Pipe[F, A, IOResult] =
    jsonEncode[F, A].andThen(_.intersperse("\n").map(ByteString(_))) >>> upload(pathStr)

  final def jackson[A: AvroEncoder: SchemaFor](pathStr: String): Pipe[F, A, IOResult] =
    jacksonEncode[F, A].andThen(_.intersperse("\n").map(ByteString(_))) >>> upload(pathStr)

  final def csv[A: HeaderEncoder](pathStr: String, conf: CsvConfiguration): Pipe[F, A, IOResult] =
    csvEncode[F, A](conf).andThen(_.intersperse("\n").map(ByteString(_))) >>> upload(pathStr)

  final def csv[A: HeaderEncoder](pathStr: String): Pipe[F, A, IOResult] =
    csv[A](pathStr, CsvConfiguration.rfc)
}

final class AkkaFtpSink[F[_]: ConcurrentEffect: ContextShift](settings: FtpSettings)(implicit
  mat: Materializer)
    extends FtpSink[F, FTPClient, FtpSettings](Ftp, settings)

final class AkkaSftpSink[F[_]: ConcurrentEffect: ContextShift](settings: SftpSettings)(implicit
  mat: Materializer)
    extends FtpSink[F, SSHClient, SftpSettings](Sftp, settings)

final class AkkaFtpsSink[F[_]: ContextShift: ConcurrentEffect](settings: FtpsSettings)(implicit
  mat: Materializer)
    extends FtpSink[F, FTPSClient, FtpsSettings](Ftps, settings)
