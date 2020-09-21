package com.github.chenharryhua.nanjin.pipes

import java.io.{ByteArrayOutputStream, EOFException, InputStream}

import cats.effect.ConcurrentEffect
import fs2.io.toInputStream
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DecoderFactory, EncoderFactory}

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
}

final class BinaryAvroDeserialization[F[_]](schema: Schema) extends Serializable {

  def deserialize(implicit F: ConcurrentEffect[F]): Pipe[F, Byte, GenericRecord] = {
    (ss: Stream[F, Byte]) =>
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
