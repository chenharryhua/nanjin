package com.github.chenharryhua.nanjin.spark

import akka.stream.IOResult
import akka.stream.alpakka.ftp.RemoteFileSettings
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.devices.FtpUploader
import com.github.chenharryhua.nanjin.pipes.{
  AvroSerialization,
  CirceSerialization,
  CsvSerialization,
  TextSerialization
}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import fs2.{Pipe, Stream}
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}

final class FtpSink[F[_], C, S <: RemoteFileSettings](uploader: FtpUploader[F, C, S]) {

  def csv[A: RowEncoder](pathStr: String, csvConfig: CsvConfiguration)(implicit
    cr: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, IOResult] = {
    val pipe = new CsvSerialization[F, A](csvConfig)
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(uploader.upload(pathStr))
  }

  def csv[A: RowEncoder](
    pathStr: String)(implicit cr: Concurrent[F], cs: ContextShift[F]): Pipe[F, A, IOResult] =
    csv[A](pathStr, CsvConfiguration.rfc)

  def json[A: JsonEncoder](pathStr: String): Pipe[F, A, IOResult] = {
    val pipe = new CirceSerialization[F, A]
    (ss: Stream[F, A]) => ss.through(pipe.serialize).through(uploader.upload(pathStr))
  }

  def jackson[A: AvroEncoder](pathStr: String)(implicit
    cs: ContextShift[F],
    ce: ConcurrentEffect[F]): Pipe[F, A, IOResult] = {
    val pipe = new AvroSerialization[F, A]
    (ss: Stream[F, A]) => ss.through(pipe.toByteJson).through(uploader.upload(pathStr))
  }

  def text(pathStr: String): Pipe[F, String, IOResult] = {
    val pipe = new TextSerialization[F]
    (ss: Stream[F, String]) => ss.through(pipe.serialize).through(uploader.upload(pathStr))
  }
}
