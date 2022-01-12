package com.github.chenharryhua.nanjin.spark.ftp

import akka.stream.alpakka.ftp.RemoteFileSettings
import akka.stream.{IOResult, Materializer}
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.pipes.serde.*
import com.github.chenharryhua.nanjin.terminals.FtpUploader
import com.sksamuel.avro4s.Encoder as AvroEncoder
import fs2.Pipe
import io.circe.Encoder as JsonEncoder
import kantan.csv.{CsvConfiguration, RowEncoder}
import squants.information.Information

final class FtpSink[F[_], C, S <: RemoteFileSettings](uploader: FtpUploader[F, C, S]) {

  def csv[A](pathStr: String, csvConfig: CsvConfiguration, byteBuffer: Information)(implicit
    enc: RowEncoder[A],
    F: Async[F],
    mat: Materializer): Pipe[F, A, IOResult] = {
    val pipe = new CsvSerialization[F, A](csvConfig)
    _.through(pipe.serialize(byteBuffer)).through(uploader.upload(pathStr))
  }

  def csv[A](pathStr: String, byteBuffer: Information)(implicit
    enc: RowEncoder[A],
    F: Async[F],
    mat: Materializer): Pipe[F, A, IOResult] =
    csv[A](pathStr, CsvConfiguration.rfc, byteBuffer)

  def json[A: JsonEncoder](pathStr: String, isKeepNull: Boolean = true)(implicit
    F: Async[F],
    mat: Materializer): Pipe[F, A, IOResult] = {
    val pipe = new CirceSerialization[F, A]
    _.through(pipe.serialize(isKeepNull)).through(uploader.upload(pathStr))
  }

  def jackson[A](pathStr: String, enc: AvroEncoder[A], chunkSize: ChunkSize)(implicit
    F: Async[F],
    mat: Materializer): Pipe[F, A, IOResult] = {
    val pipe = new JacksonSerialization[F](enc.schema)
    val gr   = new GenericRecordCodec[F, A]
    _.through(gr.encode(enc)).through(pipe.serialize(chunkSize)).through(uploader.upload(pathStr))
  }

  def text(pathStr: String)(implicit F: Async[F], mat: Materializer): Pipe[F, String, IOResult] = {
    val pipe = new TextSerialization[F]
    _.through(pipe.serialize).through(uploader.upload(pathStr))
  }
}
