package com.github.chenharryhua.nanjin

import java.io.ByteArrayOutputStream

import cats.effect.Sync
import cats.implicits._
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroOutputStream,
  DefaultFieldMapper,
  SchemaFor,
  Decoder => AvroDecoder,
  Encoder => AvroEncoder
}
import fs2.{Pipe, RaiseThrowable, Stream}
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import kantan.csv.{RowDecoder, RowEncoder}
import org.apache.avro.generic.GenericRecord

package object pipes {

  def jsonDecode[F[_]: RaiseThrowable, A: JsonDecoder]: Pipe[F, String, A] =
    (ss: Stream[F, String]) => ss.map(decode[A]).rethrow

  def jsonEncode[F[_], A: JsonEncoder]: Pipe[F, A, String] =
    (ss: Stream[F, A]) => ss.map(_.asJson.noSpaces)

  def csvDecode[F[_]: RaiseThrowable, A: RowDecoder]: Pipe[F, Seq[String], A] =
    (ss: Stream[F, Seq[String]]) => ss.map(RowDecoder[A].decode).rethrow

  def csvEncode[F[_], A: RowEncoder]: Pipe[F, A, Seq[String]] =
    (ss: Stream[F, A]) => ss.map(RowEncoder[A].encode)

  def jacksonDecode[F[_]: RaiseThrowable, A: AvroDecoder: SchemaFor]: Pipe[F, String, A] =
    (ss: Stream[F, String]) =>
      ss.map(m =>
        AvroInputStream
          .json[A]
          .from(m.getBytes)
          .build(SchemaFor[A].schema(DefaultFieldMapper))
          .iterator
          .next)

  def jacksonEncode[F[_], A: AvroEncoder: SchemaFor](implicit F: Sync[F]): Pipe[F, A, String] =
    (ss: Stream[F, A]) =>
      ss.evalMap { m =>
        F.bracket(F.pure(new ByteArrayOutputStream))(os =>
          F.pure(
              AvroOutputStream
                .json[A]
                .to(os)
                .build(SchemaFor[A].schema(DefaultFieldMapper))
                .write(m))
            .as(os.toString))(a => F.delay(a.close()))
      }

  def avroDecode[F[_]: RaiseThrowable, A: AvroDecoder: SchemaFor]: Pipe[F, GenericRecord, A] =
    (ss: Stream[F, GenericRecord]) =>
      ss.map(m =>
        AvroDecoder[A].decode(m, SchemaFor[A].schema(DefaultFieldMapper), DefaultFieldMapper))

  def avroEncode[F[_]: RaiseThrowable, A: AvroEncoder: SchemaFor]: Pipe[F, A, GenericRecord] =
    (ss: Stream[F, A]) =>
      ss.map(m =>
          AvroEncoder[A]
            .encode(m, SchemaFor[A].schema(DefaultFieldMapper), DefaultFieldMapper) match {
            case gr: GenericRecord => Right(gr)
            case _                 => Left(new Exception("not an Avro Generic Record"))
          })
        .rethrow
}
