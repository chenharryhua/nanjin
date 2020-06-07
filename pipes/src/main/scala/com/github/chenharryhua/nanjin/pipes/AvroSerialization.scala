package com.github.chenharryhua.nanjin.pipes

import java.io.InputStream

import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import cats.implicits._
import com.fasterxml.jackson.databind.ObjectMapper
import com.sksamuel.avro4s.{
  AvroFormat,
  AvroInputStream,
  AvroInputStreamBuilder,
  AvroOutputStreamBuilder,
  BinaryFormat,
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
  private val chunkSize: Int = 2048

  // serialize
  private def serialize(fmt: AvroFormat): Stream[F, A] => Stream[F, Byte] = { (ss: Stream[F, A]) =>
    readOutputStream[F](blocker, chunkSize) { os =>
      val aos = new AvroOutputStreamBuilder[A](fmt).to(os).build()
      ss.chunkN(1024, allowFewer = true).map(_.foreach(aos.write)).compile.drain
    }
  }

  private val pretty: Pipe[F, String, String] = {
    val mapper: ObjectMapper = new ObjectMapper()
    _.map(s => jacksonToCirce(mapper.readTree(s)).printWith(Printer.spaces2))
  }

  val toByteJson: Stream[F, A] => Stream[F, Byte] = serialize(JsonFormat)
  val toBinary: Pipe[F, A, Byte]                  = serialize(BinaryFormat)

  val toCompactJson: Pipe[F, A, String] = toByteJson >>> utf8Decode[F] >>> lines[F]
  val toPrettyJson: Pipe[F, A, String]  = toByteJson >>> utf8Decode[F] >>> lines[F] >>> pretty
}

final class AvroDeserialization[F[_]: ConcurrentEffect, A: AvroDecoder](blocker: Blocker) {

  private def deserialize(fmt: AvroFormat, is: InputStream): Stream[F, A] = {
    val schema: Schema          = AvroDecoder[A].schema
    val ais: AvroInputStream[A] = new AvroInputStreamBuilder[A](fmt).from(is).build(schema)
    Stream.fromIterator[F](ais.iterator)
  }

  def fromJson(is: InputStream): Stream[F, A]   = deserialize(JsonFormat, is)
  def fromBinary(is: InputStream): Stream[F, A] = deserialize(BinaryFormat, is)

  def fromJson: Pipe[F, Byte, A]   = _.through(toInputStream[F]).flatMap(fromJson)
  def fromBinary: Pipe[F, Byte, A] = _.through(toInputStream[F]).flatMap(fromBinary)
}
