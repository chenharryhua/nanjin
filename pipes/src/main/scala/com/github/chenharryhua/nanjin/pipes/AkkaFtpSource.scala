package com.github.chenharryhua.nanjin.pipes

import akka.stream.Materializer
import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.util.ByteString
import cats.effect.{Async, Concurrent, ContextShift}
import cats.implicits._
import com.sksamuel.avro4s.{SchemaFor, Decoder => AvroDecoder}
import fs2.{RaiseThrowable, Stream}
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.{CsvConfiguration, RowDecoder}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}
import streamz.converter._

sealed class FtpSource[F[_]: ContextShift: Concurrent, C, S <: RemoteFileSettings](
  ftpApi: FtpApi[C, S],
  settings: S)(implicit mat: Materializer) {

  final def download(pathStr: String): Stream[F, ByteString] = {
    val run = ftpApi.fromPath(pathStr, settings).toStreamMat[F].map {
      case (s, f) =>
        s.concurrently(Stream.eval(Async.fromFuture(Async[F].pure(f))))
    }
    Stream.force(run)
  }

  final def json[A: JsonDecoder](pathStr: String)(implicit ev: RaiseThrowable[F]): Stream[F, A] =
    download(pathStr).map(_.utf8String).through(fs2.text.lines).through(jsonDecode[F, A])

  final def jackson[A: AvroDecoder: SchemaFor](pathStr: String): Stream[F, A] =
    download(pathStr).map(_.utf8String).through(fs2.text.lines).through(jacksonDecode[F, A])

  final def csv[A: RowDecoder](pathStr: String, conf: CsvConfiguration)(implicit
    ev: RaiseThrowable[F]): Stream[F, A] = {
    val data =
      if (conf.hasHeader)
        download(pathStr).map(_.utf8String).through(fs2.text.lines).drop(1)
      else
        download(pathStr).map(_.utf8String).through(fs2.text.lines)
    data.through(csvDecode[F, A](conf))
  }

  final def csv[A: RowDecoder](pathStr: String)(implicit ev: RaiseThrowable[F]): Stream[F, A] =
    csv[A](pathStr, CsvConfiguration.rfc)

}

final class AkkaFtpSource[F[_]: ContextShift: Concurrent](settings: FtpSettings)(implicit
  mat: Materializer)
    extends FtpSource[F, FTPClient, FtpSettings](Ftp, settings)

final class AkkaSftpSource[F[_]: ContextShift: Concurrent](settings: SftpSettings)(implicit
  mat: Materializer)
    extends FtpSource[F, SSHClient, SftpSettings](Sftp, settings)

final class AkkaFtpsSource[F[_]: ContextShift: Concurrent](settings: FtpsSettings)(implicit
  mat: Materializer)
    extends FtpSource[F, FTPSClient, FtpsSettings](Ftps, settings)
