package com.github.chenharryhua.nanjin.pipes

import cats.effect.kernel.Async
import fs2.io.toInputStream
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DecoderFactory, EncoderFactory}

import java.io.{ByteArrayOutputStream, EOFException}

final class BinaryAvroSerialization[F[_]](schema: Schema) extends Serializable {

  def serialize: Pipe[F, GenericRecord, Byte] = { (ss: Stream[F, GenericRecord]) =>
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    ss.chunkN(chunkSize).flatMap { grs =>
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream()
      val encoder: BinaryEncoder      = EncoderFactory.get().binaryEncoder(baos, null)
      grs.foreach(gr => datumWriter.write(gr, encoder))
      encoder.flush()
      baos.close()
      Stream.emits(baos.toByteArray)
    }
  }

  def deserialize(implicit F: Async[F]): Pipe[F, Byte, GenericRecord] = { (ss: Stream[F, Byte]) =>
    ss.through(toInputStream).flatMap { is =>
      val avroDecoder = DecoderFactory.get().binaryDecoder(is, null)
      def pullAll(
        reader: GenericDatumReader[GenericRecord]): Pull[F, GenericRecord, Option[GenericDatumReader[GenericRecord]]] =
        Pull
          .functionKInstance(F.blocking(try Some(reader.read(null, avroDecoder))
          catch { case ex: EOFException => None }))
          .flatMap {
            case Some(a) => Pull.output1(a) >> Pull.pure(Some(reader))
            case None    => Pull.eval(F.blocking(is.close())) >> Pull.pure(None)
          }
      Pull.loop(pullAll)(new GenericDatumReader[GenericRecord](schema)).void.stream
    }
  }
}
