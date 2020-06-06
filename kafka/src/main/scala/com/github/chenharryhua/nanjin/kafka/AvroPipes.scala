package com.github.chenharryhua.nanjin.kafka

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.fasterxml.jackson.databind.ObjectMapper
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroOutputStream,
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

  private val avroJson: Stream[F, A] => Stream[F, Byte] =
    _.flatMap { cr =>
      readOutputStream(blocker, chunkSize) { os =>
        blocker.delay {
          val out: AvroOutputStream[A] = AvroOutputStream.json[A].to(os).build
          out.write(cr)
          out.flush()
          os.write("\n\r".getBytes)
          out.close()
        }
      }
    }

  val toData: Pipe[F, A, Byte] =
    _.flatMap { cr =>
      readOutputStream(blocker, chunkSize) { os =>
        blocker.delay {
          val out: AvroOutputStream[A] = AvroOutputStream.data[A].to(os).build
          out.write(cr)
          out.close()
        }
      }
    }

  val toBinary: Pipe[F, A, Byte] =
    _.flatMap { cr =>
      readOutputStream(blocker, chunkSize) { os =>
        blocker.delay {
          val out: AvroOutputStream[A] = AvroOutputStream.binary[A].to(os).build
          out.write(cr)
          out.close()
        }
      }
    }

  private val mapper: ObjectMapper = new ObjectMapper()

  private val pretty: Pipe[F, String, String] =
    _.map(s => jacksonToCirce(mapper.readTree(s)).printWith(Printer.spaces2))

  val toJackson: Pipe[F, A, String]       = avroJson >>> utf8Decode[F] >>> lines[F]
  val toPrettyJackson: Pipe[F, A, String] = avroJson >>> utf8Decode[F] >>> lines[F] >>> pretty

  def fromJackson: Pipe[F, String, A] =
    _.flatMap { cr =>
      Stream.fromIterator[F](
        AvroInputStream.json[A].from(cr.getBytes).build(AvroDecoder[A].schema).iterator)
    }

  def fromData: Pipe[F, Array[Byte], A] =
    _.flatMap { cr =>
      Stream.fromIterator[F](AvroInputStream.data[A].from(cr).build(AvroDecoder[A].schema).iterator)
    }

  def fromBinary: Pipe[F, Array[Byte], A] =
    _.flatMap { cr =>
      Stream.fromIterator[F](
        AvroInputStream.binary[A].from(cr).build(AvroDecoder[A].schema).iterator)
    }

}
