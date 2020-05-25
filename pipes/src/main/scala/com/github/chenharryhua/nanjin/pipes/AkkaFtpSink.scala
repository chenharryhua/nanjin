package com.github.chenharryhua.nanjin.pipes

import akka.stream.alpakka.ftp.FtpSettings
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import cats.effect.{ConcurrentEffect, ContextShift}
import com.sksamuel.avro4s.{SchemaFor, Encoder => AvroEncoder}
import fs2.Pipe
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import streamz.converter._

final class AkkaFtpSink[F[_]: ConcurrentEffect: ContextShift](settings: FtpSettings)(implicit
  materializer: Materializer) {
  import materializer.executionContext

  def upload(pathStr: String): Pipe[F, ByteString, IOResult] =
    Ftp.toPath(pathStr, settings).toPipeMat[F].andThen(_.rethrow)

  def json[A: JsonEncoder](pathStr: String): Pipe[F, A, IOResult] =
    upload(pathStr).compose(jsonEncode[F, A].andThen(_.intersperse("\n").map(ByteString(_))))

  def jackson[A: AvroEncoder: SchemaFor](pathStr: String): Pipe[F, A, IOResult] =
    upload(pathStr).compose(jacksonEncode[F, A].andThen(_.intersperse("\n").map(ByteString(_))))

  def csv[A: HeaderEncoder](pathStr: String, rfc: CsvConfiguration): Pipe[F, A, IOResult] =
    upload(pathStr).compose(csvEncode[F, A](rfc).andThen(_.intersperse("\n").map(ByteString(_))))
}
