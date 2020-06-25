package com.github.chenharryhua.nanjin.pipes

import java.io.{EOFException, InputStream}

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift}
import cats.implicits._
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

final class BinaryAvroSerialization[F[_]: ContextShift: Concurrent](schema: Schema) {

  def serialize: Pipe[F, GenericRecord, Byte] = { (ss: Stream[F, GenericRecord]) =>
    for {
      blocker <- Stream.resource(Blocker[F])
      byte <- readOutputStream[F](blocker, chunkSize) { os =>
        val datumWriter = new GenericDatumWriter[GenericRecord](schema)
        val encoder     = EncoderFactory.get().binaryEncoder(os, null)

        def go(as: Stream[F, GenericRecord]): Pull[F, Byte, Unit] =
          as.pull.uncons.flatMap {
            case Some((hl, tl)) =>
              Pull.eval(hl.traverse(a => blocker.delay(datumWriter.write(a, encoder)))) >> go(tl)
            case None => Pull.eval(blocker.delay(encoder.flush())) >> Pull.done
          }
        go(ss).stream.compile.drain
      }
    } yield byte
  }
}

final class BinaryAvroDeserialization[F[_]: ConcurrentEffect](schema: Schema) {
  private val F: ConcurrentEffect[F] = ConcurrentEffect[F]

  def deserialize: Pipe[F, Byte, GenericRecord] = { (ss: Stream[F, Byte]) =>
    ss.through(toInputStream).flatMap { is =>
      val avroDecoder = DecoderFactory.get().binaryDecoder(is, null)
      val datumReader = new GenericDatumReader[GenericRecord](schema)
      def pullAll(is: InputStream): Pull[F, GenericRecord, Option[InputStream]] =
        Pull
          .functionKInstance(F.delay(try Some(datumReader.read(null, avroDecoder))
          catch { case ex: EOFException => None }))
          .flatMap {
            case Some(a) => Pull.output1(a) >> Pull.pure(Some(is))
            case None    => Pull.eval(F.delay(is.close())) >> Pull.pure(None)
          }
      Pull.loop(pullAll)(is).void.stream
    }
  }
}
