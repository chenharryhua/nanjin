package com.github.chenharryhua.nanjin.pipes

import cats.ApplicativeError
import com.sksamuel.avro4s.{ToRecord, Decoder => AvroDecoder, Encoder => AvroEncoder}
import fs2.{Pipe, Stream}
import org.apache.avro.generic.GenericRecord

final class GenericRecordEncoder[F[_], A](enc: AvroEncoder[A])(implicit
  F: ApplicativeError[F, Throwable]) {
  private val to: ToRecord[A] = ToRecord(enc)

  def encode: Pipe[F, A, GenericRecord] = (ss: Stream[F, A]) => ss.map(to.to)
}

final class GenericRecordDecoder[F[_], A](dec: AvroDecoder[A]) {

  def decode: Pipe[F, GenericRecord, A] =
    (ss: Stream[F, GenericRecord]) => ss.map(rec => dec.decode(rec))

}
