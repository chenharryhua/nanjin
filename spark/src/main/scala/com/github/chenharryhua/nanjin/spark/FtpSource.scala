package com.github.chenharryhua.nanjin.spark

import akka.stream.alpakka.ftp.RemoteFileSettings
import cats.effect.{ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.devices.FtpDownloader
import com.github.chenharryhua.nanjin.pipes.{
  AvroDeserialization,
  CirceDeserialization,
  CsvDeserialization
}
import com.sksamuel.avro4s.{Decoder => AvroDecoder}
import fs2.{RaiseThrowable, Stream}
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.{CsvConfiguration, RowDecoder}

final class FtpSource[F[_], C, S <: RemoteFileSettings](downloader: FtpDownloader[F, C, S]) {

  def csv[A: RowDecoder](pathStr: String, csvConfig: CsvConfiguration)(implicit
    r: RaiseThrowable[F]): Stream[F, A] = {
    val pipe = new CsvDeserialization[F, A](csvConfig)
    downloader.download(pathStr).through(pipe.deserialize)
  }

  def csv[A: RowDecoder](pathStr: String)(implicit r: RaiseThrowable[F]): Stream[F, A] =
    csv[A](pathStr, CsvConfiguration.rfc)

  def json[A: JsonDecoder](pathStr: String)(implicit F: RaiseThrowable[F]): Stream[F, A] = {
    val pipe = new CirceDeserialization[F, A]
    downloader.download(pathStr).through(pipe.deserialize)
  }

  def jackson[A: AvroDecoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, A] = {
    val pipe = new AvroDeserialization[F, A]
    downloader.download(pathStr).through(pipe.fromJackson)
  }

}
