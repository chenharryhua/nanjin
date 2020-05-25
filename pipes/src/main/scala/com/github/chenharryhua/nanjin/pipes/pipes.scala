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
import fs2.{Pipe, Pure, RaiseThrowable, Stream}
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import kantan.csv.{CsvConfiguration, HeaderEncoder, RowDecoder}
import org.apache.avro.generic.GenericRecord

package object pipes {

  def jsonDecode[F[_]: RaiseThrowable, A: JsonDecoder]: Pipe[F, String, A] =
    (ss: Stream[F, String]) => ss.map(decode[A]).rethrow

  def jsonEncode[F[_], A: JsonEncoder]: Pipe[F, A, String] =
    (ss: Stream[F, A]) => ss.map(_.asJson.noSpaces)

  def csvDecode[F[_]: RaiseThrowable, A: RowDecoder]: Pipe[F, Seq[String], A] =
    (ss: Stream[F, Seq[String]]) => ss.map(RowDecoder[A].decode).rethrow

  def csvEncode[F[_], A: HeaderEncoder](conf: CsvConfiguration): Pipe[F, A, String] =
    (ss: Stream[F, A]) => {
      val hs: Stream[Pure, String] = conf.header match {
        case CsvConfiguration.Header.Implicit =>
          HeaderEncoder[A].header
            .map(h => Stream(h.mkString(conf.cellSeparator.toString)))
            .getOrElse(Stream())
        case CsvConfiguration.Header.Explicit(seq) =>
          Stream(seq.mkString(conf.cellSeparator.toString))
        case CsvConfiguration.Header.None => Stream()
      }
      hs.covary[F] ++ ss.map(m =>
        HeaderEncoder[A].rowEncoder.encode(m).mkString(conf.cellSeparator.toString))
    }

  def jacksonDecode[F[_], A: AvroDecoder: SchemaFor]: Pipe[F, String, A] =
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
      ss.map { m =>
        val bos = new ByteArrayOutputStream
        val aos = AvroOutputStream.json[A].to(bos).build(SchemaFor[A].schema(DefaultFieldMapper))
        aos.write(m)
        aos.close()
        bos.close()
        bos.toString
      }

  def avroDecode[F[_], A: AvroDecoder: SchemaFor]: Pipe[F, GenericRecord, A] =
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
