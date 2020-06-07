package com.github.chenharryhua.nanjin.kafka

import java.io.InputStream

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.fasterxml.jackson.databind.ObjectMapper
import com.sksamuel.avro4s.{
  AvroFormat,
  AvroInputStreamBuilder,
  AvroOutputStream,
  AvroOutputStreamBuilder,
  BinaryFormat,
  DataFormat,
  JsonFormat,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import fs2.io.readOutputStream
import fs2.text.{lines, utf8Decode}
import fs2.{Pipe, Stream}
import io.circe.Printer
import io.circe.jackson.jacksonToCirce

final class AvroPipes[F[_]: ContextShift: Concurrent, A: AvroEncoder: AvroDecoder](
  blocker: Blocker) {
  private val chunkSize = 8192

  // encode
  private def encode(fmt: AvroFormat): Stream[F, A] => Stream[F, Byte] = { (ss: Stream[F, A]) =>
    readOutputStream[F](blocker, chunkSize) { os =>
      val builder: AvroOutputStreamBuilder[A] = new AvroOutputStreamBuilder[A](fmt)
      val output: AvroOutputStream[A]         = builder.to(os).build
      ss.map(m => output.write(m)).compile.drain
    }
  }

  private val pretty: Pipe[F, String, String] = {
    val mapper: ObjectMapper = new ObjectMapper()
    _.map(s => jacksonToCirce(mapper.readTree(s)).printWith(Printer.spaces2))
  }

  val toByteJson: Stream[F, A] => Stream[F, Byte] = encode(JsonFormat)
  val toData: Pipe[F, A, Byte]                    = encode(DataFormat)
  val toBinary: Pipe[F, A, Byte]                  = encode(BinaryFormat)

  val toCompactJson: Pipe[F, A, String] = toByteJson >>> utf8Decode[F] >>> lines[F]
  val toPrettyJson: Pipe[F, A, String]  = toByteJson >>> utf8Decode[F] >>> lines[F] >>> pretty

  // decode
  private def decode(fmt: AvroFormat, is: InputStream): Stream[F, A] = {
    val builder = new AvroInputStreamBuilder[A](fmt)
    val schema  = AvroDecoder[A].schema
    Stream.fromIterator[F](builder.from(is).build(schema).iterator)
  }

  def fromJson(is: InputStream): Stream[F, A]   = decode(JsonFormat, is)
  def fromData(is: InputStream): Stream[F, A]   = decode(DataFormat, is)
  def fromBinary(is: InputStream): Stream[F, A] = decode(BinaryFormat, is)
}
