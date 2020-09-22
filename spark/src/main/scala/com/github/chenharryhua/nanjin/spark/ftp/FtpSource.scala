package com.github.chenharryhua.nanjin.spark.ftp

import akka.stream.alpakka.ftp.RemoteFileSettings
import cats.effect.{ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.devices.FtpDownloader
import com.github.chenharryhua.nanjin.pipes._
import com.sksamuel.avro4s.{Decoder => AvroDecoder}
import fs2.Stream
import io.circe.{Decoder => JsonDecoder}
import kantan.csv.{CsvConfiguration, RowDecoder}

final class FtpSource[F[_], C, S <: RemoteFileSettings](downloader: FtpDownloader[F, C, S]) {

  def csv[A](pathStr: String, csvConfig: CsvConfiguration)(implicit
    dec: RowDecoder[A],
    r: ConcurrentEffect[F],
    cs: ContextShift[F]): Stream[F, A] = {
    val pipe = new CsvSerialization[F, A](csvConfig)
    downloader.download(pathStr).through(pipe.deserialize)
  }

  def csv[A](pathStr: String)(implicit
    dec: RowDecoder[A],
    r: ConcurrentEffect[F],
    cs: ContextShift[F]): Stream[F, A] =
    csv[A](pathStr, CsvConfiguration.rfc)

  def json[A: JsonDecoder](
    pathStr: String)(implicit r: ConcurrentEffect[F], cs: ContextShift[F]): Stream[F, A] = {
    val pipe = new CirceSerialization[F, A]
    downloader.download(pathStr).through(pipe.deserialize)
  }

  def jackson[A: AvroDecoder](
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, A] = {
    val pipe = new JacksonSerialization[F](AvroDecoder[A].schema)
    val gr   = new GenericRecordCodec[F, A]
    downloader.download(pathStr).through(pipe.deserialize).through(gr.decode)
  }

  def text(
    pathStr: String)(implicit cs: ContextShift[F], ce: ConcurrentEffect[F]): Stream[F, String] = {
    val pipe = new TextSerialization[F]
    downloader.download(pathStr).through(pipe.deserialize)
  }
}
