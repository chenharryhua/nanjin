package com.github.chenharryhua.nanjin.terminals

import akka.stream.Materializer
import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.stream.scaladsl.Sink
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.Stream
import fs2.interop.reactivestreams.PublisherOps
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

sealed abstract class FtpDownloader[F[_], C, S <: RemoteFileSettings](
  ftpApi: FtpApi[C, S],
  settings: S,
  chunkSize: ChunkSize) {

  final def withChunkSize(cs: ChunkSize): FtpDownloader[F, C, S] = new FtpDownloader[F, C, S](ftpApi, settings, cs) {}

  final def download(pathStr: String)(implicit F: Async[F], mat: Materializer): Stream[F, Byte] =
    Stream.suspend {
      for {
        bs <- ftpApi
          .fromPath(pathStr, settings)
          .runWith(Sink.asPublisher(fanout = false))
          .toStreamBuffered(chunkSize.value)
        byte <- Stream.emits(bs)
      } yield byte
    }
}

object FtpDownloader {

  def apply[F[_]](settings: FtpSettings): FtpDownloader[F, FTPClient, FtpSettings] =
    new FtpDownloader[F, FTPClient, FtpSettings](Ftp, settings, ChunkSize(1024)) {}

  def apply[F[_]](settings: SftpSettings): FtpDownloader[F, SSHClient, SftpSettings] =
    new FtpDownloader[F, SSHClient, SftpSettings](Sftp, settings, ChunkSize(1024)) {}

  def apply[F[_]](settings: FtpsSettings): FtpDownloader[F, FTPSClient, FtpsSettings] =
    new FtpDownloader[F, FTPSClient, FtpsSettings](Ftps, settings, ChunkSize(1024)) {}

}
