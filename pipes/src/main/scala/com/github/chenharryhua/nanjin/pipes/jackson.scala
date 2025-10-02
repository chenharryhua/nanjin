package com.github.chenharryhua.nanjin.pipes

import cats.effect.kernel.Async
import fs2.io.toInputStream
import fs2.{Chunk, Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory, JsonEncoder}

import java.io.{ByteArrayOutputStream, EOFException, InputStream}
import java.nio.charset.StandardCharsets

object jackson {

  def toBytes[F[_]](schema: Schema): Pipe[F, GenericRecord, Byte] = {
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    (sfgr: Stream[F, GenericRecord]) =>
      sfgr.chunks.map { grs =>
        val baos: ByteArrayOutputStream = new ByteArrayOutputStream
        val encoder: JsonEncoder = EncoderFactory.get().jsonEncoder(schema, baos)
        grs.foreach(gr => datumWriter.write(gr, encoder))
        encoder.flush()
        baos.close()
        baos.toByteArray // JsonEncoder use ISO_8859_1
      }.intersperse(System.lineSeparator().getBytes(StandardCharsets.ISO_8859_1))
        .flatMap(ba => Stream.chunk(Chunk.from(ba.toVector)))
  }

  def fromBytes[F[_]](schema: Schema)(implicit F: Async[F]): Pipe[F, Byte, GenericRecord] = {
    (ss: Stream[F, Byte]) =>
      ss.through(toInputStream).flatMap { is =>
        val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, is)
        val datumReader = new GenericDatumReader[GenericRecord](schema)
        def pullAll(is: InputStream): Pull[F, GenericRecord, Option[InputStream]] =
          Pull
            .functionKInstance(F.blocking(try Some(datumReader.read(null, jsonDecoder))
            catch { case _: EOFException => None }))
            .flatMap {
              case Some(a) => Pull.output1[F, GenericRecord](a) >> Pull.pure[F, Option[InputStream]](Some(is))
              case None    => Pull.eval(F.blocking(is.close())) >> Pull.pure[F, Option[InputStream]](None)
            }
        Pull.loop(pullAll)(is).void.stream
      }
  }
}
