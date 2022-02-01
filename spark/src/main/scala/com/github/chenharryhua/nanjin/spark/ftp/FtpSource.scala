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
    mat: Materializer): Stream[F, A] =
    downloader.download(pathStr, chunkSize).through(CsvSerde.deserPipe[F, A](csvConfig, chunkSize))

  def csv[A](pathStr: String, chunkSize: ChunkSize)(implicit
    dec: RowDecoder[A],
    F: Async[F],
    mat: Materializer): Stream[F, A] =
    csv[A](pathStr, CsvConfiguration.rfc, chunkSize)

  def json[A: JsonDecoder](pathStr: String, chunkSize: ChunkSize)(implicit
    F: Async[F],
    mat: Materializer): Stream[F, A] =
    downloader.download(pathStr, chunkSize).through(CirceSerde.deserPipe[F, A])

  def jackson[A](pathStr: String, chunkSize: ChunkSize, dec: AvroDecoder[A])(implicit
    F: Async[F],
    mat: Materializer): Stream[F, A] =
    downloader.download(pathStr, chunkSize).through(JacksonSerde.deserPipe(dec.schema)).map(dec.decode)

  def text(pathStr: String, chunkSize: ChunkSize)(implicit F: Async[F], mat: Materializer): Stream[F, String] =
    downloader.download(pathStr, chunkSize).through(TextSerde.deserPipe)
}
