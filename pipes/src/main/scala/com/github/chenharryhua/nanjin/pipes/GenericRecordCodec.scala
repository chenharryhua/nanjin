package com.github.chenharryhua.nanjin.pipes

import cats.ApplicativeError
import com.sksamuel.avro4s.{ToRecord, Decoder => AvroDecoder, Encoder => AvroEncoder}
import fs2.{Pipe, Stream}
import org.apache.avro.generic.GenericRecord

final class GenericRecordCodec[F[_], A] extends Serializable {

  def encode(implicit
    enc: AvroEncoder[A],
    F: ApplicativeError[F, Throwable]): Pipe[F, A, GenericRecord] = {
    val to: ToRecord[A] = ToRecord(enc)
    (ss: Stream[F, A]) => ss.map(to.to)
  }

  def decode(implicit dec: AvroDecoder[A]): Pipe[F, GenericRecord, A] =
    (ss: Stream[F, GenericRecord]) => ss.map(rec => dec.decode(rec))

}
