package com.github.chenharryhua.nanjin.terminals

import akka.stream.{IOResult, Materializer}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.pipes.*
import com.sksamuel.avro4s.{Encoder as AvroEncoder, ToRecord}
import fs2.{Pipe, Stream}
import fs2.interop.reactivestreams.StreamOps
import io.circe.Encoder as JsonEncoder
import kantan.csv.{CsvConfiguration, RowEncoder}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}
import squants.information.Information

import scala.concurrent.Future

sealed abstract class FtpUploader[F[_], C, S <: RemoteFileSettings](ftpApi: FtpApi[C, S], settings: S) {

  final def upload(pathStr: String)(implicit F: Async[F], mat: Materializer): Pipe[F, Byte, IOResult] = {
    (ss: Stream[F, Byte]) =>
      Stream.eval(ss.chunks.toUnicastPublisher.use { p =>
        F.fromFuture(
          F.blocking(
            Source.fromPublisher(p).map(x => ByteString.apply(x.toArray)).runWith(akka.upload(pathStr))))
      })
  }

  def kantan[A](pathStr: String, csvConfig: CsvConfiguration, byteBuffer: Information)(implicit
    enc: RowEncoder[A],
    F: Async[F],
    mat: Materializer): Pipe[F, A, IOResult] =
    _.through(KantanSerde.toBytes[F, A](csvConfig, byteBuffer)).through(upload(pathStr))

  def kantan[A](pathStr: String, byteBuffer: Information)(implicit
    enc: RowEncoder[A],
    F: Async[F],
    mat: Materializer): Pipe[F, A, IOResult] =
    kantan[A](pathStr, CsvConfiguration.rfc, byteBuffer)

  def circe[A: JsonEncoder](pathStr: String, isKeepNull: Boolean = true)(implicit
    F: Async[F],
    mat: Materializer): Pipe[F, A, IOResult] =
    _.through(CirceSerde.toBytes[F, A](isKeepNull)).through(upload(pathStr))

  def jackson[A](pathStr: String, enc: AvroEncoder[A])(implicit
    F: Async[F],
    mat: Materializer): Pipe[F, A, IOResult] = {
    val toRec: ToRecord[A] = ToRecord(enc)
    _.map(toRec.to).through(JacksonSerde.toBytes[F](enc.schema)).through(upload(pathStr))
  }

  def text(pathStr: String)(implicit F: Async[F], mat: Materializer): Pipe[F, String, IOResult] =
    _.through(TextSerde.toBytes).through(upload(pathStr))

  object akka {
    final def upload(pathStr: String): Sink[ByteString, Future[IOResult]] =
      ftpApi.toPath(pathStr, settings)
  }
}

object FtpUploader {

  def apply[F[_]](settings: FtpSettings): FtpUploader[F, FTPClient, FtpSettings] =
    new FtpUploader[F, FTPClient, FtpSettings](Ftp, settings) {}

  def apply[F[_]](settings: SftpSettings): FtpUploader[F, SSHClient, SftpSettings] =
    new FtpUploader[F, SSHClient, SftpSettings](Sftp, settings) {}

  def apply[F[_]](settings: FtpsSettings): FtpUploader[F, FTPSClient, FtpsSettings] =
    new FtpUploader[F, FTPSClient, FtpsSettings](Ftps, settings) {}
}
