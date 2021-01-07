package com.github.chenharryhua.nanjin.spark.ftp

import akka.stream.alpakka.ftp.RemoteFileSettings
import akka.stream.{IOResult, Materializer}
import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.devices.FtpUploader
import com.github.chenharryhua.nanjin.pipes._
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import fs2.Pipe
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, RowEncoder}

final class FtpSink[F[_], C, S <: RemoteFileSettings](uploader: FtpUploader[F, C, S], blocker: Blocker) {

  def csv[A](pathStr: String, csvConfig: CsvConfiguration)(implicit
    enc: RowEncoder[A],
    cr: ConcurrentEffect[F],
    cs: ContextShift[F],
    mat: Materializer): Pipe[F, A, IOResult] = {
    val pipe = new CsvSerialization[F, A](csvConfig)
    _.through(pipe.serialize(blocker)).through(uploader.upload(pathStr))
  }

  def csv[A](pathStr: String)(implicit
    enc: RowEncoder[A],
    cr: ConcurrentEffect[F],
    cs: ContextShift[F],
    mat: Materializer): Pipe[F, A, IOResult] =
    csv[A](pathStr, CsvConfiguration.rfc)

  def json[A: JsonEncoder](pathStr: String, isKeepNull: Boolean = true)(implicit
    cr: ConcurrentEffect[F],
    cs: ContextShift[F],
    mat: Materializer): Pipe[F, A, IOResult] = {
    val pipe = new CirceSerialization[F, A]
    _.through(pipe.serialize(isKeepNull)).through(uploader.upload(pathStr))
  }

  def jackson[A](pathStr: String, enc: AvroEncoder[A])(implicit
    cs: ContextShift[F],
    ce: ConcurrentEffect[F],
    mat: Materializer): Pipe[F, A, IOResult] = {
    val pipe = new JacksonSerialization[F](enc.schema)
    val gr   = new GenericRecordCodec[F, A]
    _.through(gr.encode(enc)).through(pipe.serialize).through(uploader.upload(pathStr))
  }

  def text(pathStr: String)(implicit
    cr: ConcurrentEffect[F],
    cs: ContextShift[F],
    mat: Materializer): Pipe[F, String, IOResult] = {
    val pipe = new TextSerialization[F]
    _.through(pipe.serialize).through(uploader.upload(pathStr))
  }
}
