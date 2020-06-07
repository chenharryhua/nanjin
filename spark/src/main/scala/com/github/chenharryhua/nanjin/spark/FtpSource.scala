package com.github.chenharryhua.nanjin.spark

import akka.stream.alpakka.ftp.RemoteFileSettings
import com.github.chenharryhua.nanjin.devices.FtpDownloader
import com.github.chenharryhua.nanjin.pipes.CsvDeserialization
import fs2.{RaiseThrowable, Stream}
import kantan.csv.{CsvConfiguration, RowDecoder}

final class FtpSource[F[_], C, S <: RemoteFileSettings](downloader: FtpDownloader[F, C, S]) {

  def csv[A: RowDecoder](pathStr: String, csvConfig: CsvConfiguration)(implicit
    r: RaiseThrowable[F]): Stream[F, A] = {
    val pipe = new CsvDeserialization[F, A](csvConfig)
    downloader.download(pathStr).through(pipe.deserialize)
  }

  def csv[A: RowDecoder](pathStr: String)(implicit r: RaiseThrowable[F]): Stream[F, A] =
    csv[A](pathStr, CsvConfiguration.rfc)

}
