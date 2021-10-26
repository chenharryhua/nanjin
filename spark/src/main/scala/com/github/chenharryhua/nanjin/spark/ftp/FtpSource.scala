package com.github.chenharryhua.nanjin.spark.ftp

import akka.stream.Materializer
import akka.stream.alpakka.ftp.RemoteFileSettings
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.pipes.serde.{
  CirceSerialization,
  CsvSerialization,
  GenericRecordCodec,
  JacksonSerialization,
  TextSerialization
}
import com.github.chenharryhua.nanjin.terminals.FtpDownloader
import com.sksamuel.avro4s.Decoder as AvroDecoder
import fs2.Stream
import io.circe.Decoder as JsonDecoder
import kantan.csv.{CsvConfiguration, RowDecoder}

final class FtpSource[F[_], C, S <: RemoteFileSettings](downloader: FtpDownloader[F, C, S]) {

  def csv[A](pathStr: String, csvConfig: CsvConfiguration, chunkSize: Int)(implicit
    dec: RowDecoder[A],
    F: Async[F],
    mat: Materializer): Stream[F, A] = {
    val pipe = new CsvSerialization[F, A](csvConfig, chunkSize)
    downloader.download(pathStr).through(pipe.deserialize)
  }

  def csv[A](pathStr: String, chunkSize: Int)(implicit
    dec: RowDecoder[A],
    F: Async[F],
    mat: Materializer): Stream[F, A] =
    csv[A](pathStr, CsvConfiguration.rfc, chunkSize)

  def json[A: JsonDecoder](pathStr: String)(implicit F: Async[F], mat: Materializer): Stream[F, A] = {
    val pipe = new CirceSerialization[F, A]
    downloader.download(pathStr).through(pipe.deserialize)
  }

  def jackson[A](pathStr: String, dec: AvroDecoder[A])(implicit F: Async[F], mat: Materializer): Stream[F, A] = {
    val pipe = new JacksonSerialization[F](dec.schema)
    val gr   = new GenericRecordCodec[F, A]
    downloader.download(pathStr).through(pipe.deserialize).through(gr.decode(dec))
  }

  def text(pathStr: String)(implicit F: Async[F], mat: Materializer): Stream[F, String] = {
    val pipe = new TextSerialization[F]
    downloader.download(pathStr).through(pipe.deserialize)
  }
}
