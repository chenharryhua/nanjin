package com.github.chenharryhua.nanjin.pipes

import java.io.InputStream

import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
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
import fs2.io.{readOutputStream, toInputStream}
import fs2.text.{lines, utf8Decode}
import fs2.{Pipe, Stream}
import io.circe.Printer
import io.circe.jackson.jacksonToCirce
import org.apache.avro.Schema

final class AvroSerialization[F[_]: ContextShift: ConcurrentEffect, A: AvroEncoder](
  blocker: Blocker) {
  private val chunkSize = 8192

  // serialize
  private def serialize(fmt: AvroFormat): Stream[F, A] => Stream[F, Byte] = { (ss: Stream[F, A]) =>
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

  val toByteJson: Stream[F, A] => Stream[F, Byte] = serialize(JsonFormat)
  val toData: Pipe[F, A, Byte]                    = serialize(DataFormat)
  val toBinary: Pipe[F, A, Byte]                  = serialize(BinaryFormat)

  val toCompactJson: Pipe[F, A, String] = toByteJson >>> utf8Decode[F] >>> lines[F]
  val toPrettyJson: Pipe[F, A, String]  = toByteJson >>> utf8Decode[F] >>> lines[F] >>> pretty
}

final class AvroDeserialization[F[_]: ContextShift: ConcurrentEffect, A: AvroDecoder](
  blocker: Blocker) {

  private val schema: Schema = AvroDecoder[A].schema

  private def deserialize(fmt: AvroFormat, is: InputStream): Stream[F, A] = {
    val builder = new AvroInputStreamBuilder[A](fmt)
    Stream.fromIterator[F](builder.from(is).build(schema).iterator)
  }

  def fromJson(is: InputStream): Stream[F, A]   = deserialize(JsonFormat, is)
  def fromData(is: InputStream): Stream[F, A]   = deserialize(DataFormat, is)
  def fromBinary(is: InputStream): Stream[F, A] = deserialize(BinaryFormat, is)

  def fromJson: Pipe[F, Byte, A]   = _.through(toInputStream[F]).flatMap(fromJson)
  def fromData: Pipe[F, Byte, A]   = _.through(toInputStream[F]).flatMap(fromData)
  def fromBinary: Pipe[F, Byte, A] = _.through(toInputStream[F]).flatMap(fromBinary)

}
