package com.github.chenharryhua.nanjin.terminals

import akka.stream.alpakka.ftp.scaladsl.{Ftp, FtpApi, Ftps, Sftp}
import akka.stream.alpakka.ftp.{FtpSettings, FtpsSettings, RemoteFileSettings, SftpSettings}
import akka.stream.scaladsl.Source
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import cats.effect.{Async, ConcurrentEffect}
import fs2.interop.reactivestreams._
import fs2.{Pipe, Stream}
import net.schmizz.sshj.SSHClient
import org.apache.commons.net.ftp.{FTPClient, FTPSClient}

sealed abstract class FtpUploader[F[_], C, S <: RemoteFileSettings](ftpApi: FtpApi[C, S], settings: S) {

  final def upload(pathStr: String)(implicit
    F: ConcurrentEffect[F],
    mat: Materializer): Pipe[F, Byte, IOResult] = { (ss: Stream[F, Byte]) =>
    val sink   = ftpApi.toPath(pathStr, settings)
    val source = Source.fromPublisher(ss.chunkN(chunkSize).toUnicastPublisher).map(x => ByteString.apply(x.toArray))
    Stream.eval(Async.fromFuture(F.delay(source.runWith(sink))))
  }
}

final class AkkaFtpUploader[F[_]](settings: FtpSettings) extends FtpUploader[F, FTPClient, FtpSettings](Ftp, settings)

final class AkkaSftpUploader[F[_]](settings: SftpSettings)
    extends FtpUploader[F, SSHClient, SftpSettings](Sftp, settings)

final class AkkaFtpsUploader[F[_]](settings: FtpsSettings)
    extends FtpUploader[F, FTPSClient, FtpsSettings](Ftps, settings)
