package com.github.chenharryhua.nanjin.spark.ftp

import akka.stream.Materializer
import akka.stream.alpakka.ftp.RemoteFileSettings
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.pipes.serde.*
import com.github.chenharryhua.nanjin.terminals.FtpDownloader
import com.sksamuel.avro4s.Decoder as AvroDecoder
import fs2.Stream
import io.circe.Decoder as JsonDecoder
import kantan.csv.{CsvConfiguration, RowDecoder}

final class FtpSource[F[_], C, S <: RemoteFileSettings](downloader: FtpDownloader[F, C, S]) {

  def csv[A](pathStr: String, csvConfig: CsvConfiguration, chunkSize: ChunkSize)(implicit
    dec: RowDecoder[A],
    F: Async[F],
    mat: Materializer): Stream[F, A] = {
    val pipe = new CsvSerialization[F, A](csvConfig)
    downloader.download(pathStr, chunkSize).through(pipe.deserialize(chunkSize))
  }

  def csv[A](pathStr: String, chunkSize: ChunkSize)(implicit
    dec: RowDecoder[A],
    F: Async[F],
    mat: Materializer): Stream[F, A] =
    csv[A](pathStr, CsvConfiguration.rfc, chunkSize)

  def json[A: JsonDecoder](pathStr: String, chunkSize: ChunkSize)(implicit F: Async[F], mat: Materializer): Stream[F, A] = {
    val pipe: CirceSerialization[F, A] = new CirceSerialization[F, A]
    downloader.download(pathStr, chunkSize).through(pipe.deserialize)
  }

  def jackson[A](pathStr: String, chunkSize: ChunkSize, dec: AvroDecoder[A])(implicit F: Async[F], mat: Materializer): Stream[F, A] = {
    val pipe: JacksonSerialization[F] = new JacksonSerialization[F](dec.schema)
    downloader.download(pathStr, chunkSize).through(pipe.deserialize).map(dec.decode)
  }

  def text(pathStr: String, chunkSize: ChunkSize)(implicit F: Async[F], mat: Materializer): Stream[F, String] = {
    val pipe: TextSerialization[F] = new TextSerialization[F]
    downloader.download(pathStr, chunkSize).through(pipe.deserialize)
  }
}
