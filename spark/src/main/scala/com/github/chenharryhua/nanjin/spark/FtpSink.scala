package com.github.chenharryhua.nanjin.spark

import akka.stream.IOResult
import akka.stream.alpakka.ftp.RemoteFileSettings
import com.github.chenharryhua.nanjin.devices.FtpUploader
import com.github.chenharryhua.nanjin.pipes.CsvSerialization
import fs2.{Pipe, Stream}
import kantan.csv.{CsvConfiguration, HeaderEncoder}

final class FtpSink[F[_], C, S <: RemoteFileSettings](uploader: FtpUploader[F, C, S]) {

  def csv[A: HeaderEncoder](pathStr: String, csvConfig: CsvConfiguration): Pipe[F, A, IOResult] = {
    val pipe = new CsvSerialization[F, A](csvConfig)
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(uploader.upload(pathStr))
  }

  def csv[A: HeaderEncoder](pathStr: String): Pipe[F, A, IOResult] =
    csv[A](pathStr, CsvConfiguration.rfc)

}
