package com.github.chenharryhua.nanjin.pipes

import cats.ApplicativeError
import cats.implicits._
import com.sksamuel.avro4s.{Decoder => AvroDecoder, Encoder => AvroEncoder}
import fs2.{Pipe, Stream}
import org.apache.avro.generic.GenericRecord

final class GenericRecordEncoder[F[_], A](enc: AvroEncoder[A])(implicit
  F: ApplicativeError[F, Throwable]) {

  def encode: Pipe[F, A, GenericRecord] =
    (ss: Stream[F, A]) =>
      ss.evalMap { rec =>
        enc.encode(rec) match {
          case gr: GenericRecord => F.pure(gr)
          case x =>
            F.raiseError[GenericRecord](new Exception(s"not a generic record ${x.toString}"))
        }
      }
}

final class GenericRecordDecoder[F[_], A](dec: AvroDecoder[A]) {

  def decode: Pipe[F, GenericRecord, A] =
    (ss: Stream[F, GenericRecord]) => ss.map(rec => dec.decode(rec))

}
