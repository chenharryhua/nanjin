package com.github.chenharryhua.nanjin.pipes

import akka.stream.IOResult
import akka.util.ByteString
import com.sksamuel.avro4s.{SchemaFor, Encoder => AvroEncoder}
import fs2.Pipe
import io.circe.{Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, HeaderEncoder}

trait FtpSink[F[_]] {
  def upload(pathStr: String): Pipe[F, ByteString, IOResult]

  def json[A: JsonEncoder](pathStr: String): Pipe[F, A, IOResult]

  def jackson[A: AvroEncoder: SchemaFor](pathStr: String): Pipe[F, A, IOResult]

  def csv[A: HeaderEncoder](pathStr: String, conf: CsvConfiguration): Pipe[F, A, IOResult]

  def csv[A: HeaderEncoder](pathStr: String): Pipe[F, A, IOResult]
}
