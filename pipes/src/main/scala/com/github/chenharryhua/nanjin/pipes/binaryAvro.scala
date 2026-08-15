package com.github.chenharryhua.nanjin.pipes

import cats.effect.kernel.{Async, Sync}
import fs2.io.toInputStream
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DecoderFactory, EncoderFactory}

import java.io.{ByteArrayOutputStream, EOFException, InputStream}

object binaryAvro {

  def toBytes[F[_]](schema: Schema)(using F: Sync[F]): Pipe[F, GenericRecord, Byte] = {
    (ss: Stream[F, GenericRecord]) =>
      Stream.eval(F.delay(new GenericDatumWriter[GenericRecord](schema))).flatMap { datumWriter =>
        ss.chunks.flatMap { grs =>
          val baos: ByteArrayOutputStream = new ByteArrayOutputStream
          val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)
          grs.foreach(gr => datumWriter.write(gr, encoder))
          encoder.flush()
          baos.close()
          Stream.emits(baos.toByteArray)
        }
      }
  }

  def fromBytes[F[_]](schema: Schema)(using F: Async[F]): Pipe[F, Byte, GenericRecord] = {
    (ss: Stream[F, Byte]) =>
      Stream.eval(F.delay(new GenericDatumReader[GenericRecord](schema))).flatMap { datumReader =>
        ss.through(toInputStream).flatMap { is =>
          val avroDecoder = DecoderFactory.get().binaryDecoder(is, null)
          def pullAll(is: InputStream): Pull[F, GenericRecord, Option[InputStream]] =
            Pull
              .functionKInstance(
                F.delay(try Option(datumReader.read(null, avroDecoder))
                catch { case _: EOFException => None }))
              .flatMap {
                case Some(a) =>
                  Pull.output1[F, GenericRecord](a) >> Pull.pure[F, Option[InputStream]](Some(is))
                case None => Pull.eval(F.blocking(is.close())) >> Pull.pure[F, Option[InputStream]](None)
              }
          Pull.loop(pullAll)(is).void.stream
        }
      }
  }

}
