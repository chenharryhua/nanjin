package com.github.chenharryhua.nanjin.terminals

import akka.stream.{IOResult, Materializer}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.pipes.*
import com.sksamuel.avro4s.Decoder as AvroDecoder
import fs2.Stream
import fs2.interop.reactivestreams.PublisherOps
import io.circe.Decoder as JsonDecoder
import kantan.csv.{CsvConfiguration, RowDecoder}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

import scala.concurrent.Future
sealed abstract class FtpDownloader[F[_], C, S <: RemoteFileSettings](ftpApi: FtpApi[C, S], settings: S) {

  final def download(pathStr: String, chunkSize: ChunkSize)(implicit
    F: Async[F],
    mat: Materializer): Stream[F, Byte] =
    Stream.suspend {
      for {
        bs <- akka
          .download(pathStr)
          .runWith(Sink.asPublisher(fanout = false))
          .toStreamBuffered(chunkSize.value)
        byte <- Stream.emits(bs)
      } yield byte
    }

  def kantan[A](pathStr: String, csvConfig: CsvConfiguration, chunkSize: ChunkSize)(implicit
    dec: RowDecoder[A],
    F: Async[F],
    mat: Materializer): Stream[F, A] =
    download(pathStr, chunkSize).through(KantanSerde.fromBytes[F, A](csvConfig, chunkSize))

  def kantan[A](pathStr: String, chunkSize: ChunkSize)(implicit
    dec: RowDecoder[A],
    F: Async[F],
    mat: Materializer): Stream[F, A] =
    kantan[A](pathStr, CsvConfiguration.rfc, chunkSize)

  def circe[A: JsonDecoder](pathStr: String, chunkSize: ChunkSize)(implicit
    F: Async[F],
    mat: Materializer): Stream[F, A] =
    download(pathStr, chunkSize).through(CirceSerde.fromBytes[F, A])

  def jackson[A](pathStr: String, chunkSize: ChunkSize, dec: AvroDecoder[A])(implicit
    F: Async[F],
    mat: Materializer): Stream[F, A] =
    download(pathStr, chunkSize).through(JacksonSerde.fromBytes(dec.schema)).map(dec.decode)

  def text(pathStr: String, chunkSize: ChunkSize)(implicit
    F: Async[F],
    mat: Materializer): Stream[F, String] =
    download(pathStr, chunkSize).through(TextSerde.fromBytes)

  object akka {
    final def download(pathStr: String): Source[ByteString, Future[IOResult]] =
      ftpApi.fromPath(pathStr, settings)
  }
}

object FtpDownloader {

  def apply[F[_]](settings: FtpSettings): FtpDownloader[F, FTPClient, FtpSettings] =
    new FtpDownloader[F, FTPClient, FtpSettings](Ftp, settings) {}

  def apply[F[_]](settings: SftpSettings): FtpDownloader[F, SSHClient, SftpSettings] =
    new FtpDownloader[F, SSHClient, SftpSettings](Sftp, settings) {}

  def apply[F[_]](settings: FtpsSettings): FtpDownloader[F, FTPSClient, FtpsSettings] =
    new FtpDownloader[F, FTPSClient, FtpsSettings](Ftps, settings) {}

}
