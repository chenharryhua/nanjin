package com.github.chenharryhua.nanjin

import java.io.ByteArrayOutputStream

import cats.implicits._
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroOutputStream,
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

  def csvDecode[F[_]: RaiseThrowable, A: RowDecoder](conf: CsvConfiguration): Pipe[F, String, A] =
    (ss: Stream[F, String]) =>
      ss.map(r => RowDecoder[A].decode(r.split(conf.cellSeparator))).rethrow

  def csvEncode[F[_], A: HeaderEncoder](conf: CsvConfiguration): Pipe[F, A, String] =
    (ss: Stream[F, A]) => {
      val hs: Stream[Pure, String] = conf.header match {
        case CsvConfiguration.Header.Implicit =>
          HeaderEncoder[A].header match {
            case Some(h) => Stream(h.mkString(conf.cellSeparator.toString))
            case None    => Stream("")
          }
        case CsvConfiguration.Header.Explicit(seq) =>
          Stream(seq.mkString(conf.cellSeparator.toString))
        case CsvConfiguration.Header.None => Stream()
      }
      hs.covary[F] ++ ss.map(m =>
        HeaderEncoder[A].rowEncoder.encode(m).mkString(conf.cellSeparator.toString))
    }

  def jacksonDecode[F[_], A: AvroDecoder]: Pipe[F, String, A] =
    (ss: Stream[F, String]) =>
      ss.map(m =>
        AvroInputStream.json[A].from(m.getBytes).build(AvroDecoder[A].schema).iterator.next)

  def jacksonEncode[F[_], A: AvroEncoder]: Pipe[F, A, String] =
    (ss: Stream[F, A]) =>
      ss.map { m =>
        val bos = new ByteArrayOutputStream
        val aos = AvroOutputStream.json[A].to(bos).build
        aos.write(m)
        aos.close()
        bos.close()
        bos.toString
      }

  def avroDecode[F[_], A: AvroDecoder]: Pipe[F, GenericRecord, A] =
    (ss: Stream[F, GenericRecord]) => ss.map(m => AvroDecoder[A].decode(m))

  def avroEncode[F[_]: RaiseThrowable, A: AvroEncoder]: Pipe[F, A, GenericRecord] =
    (ss: Stream[F, A]) =>
      ss.map(m =>
          AvroEncoder[A].encode(m) match {
            case gr: GenericRecord => Right(gr)
            case _                 => Left(new Exception("not an Avro Generic Record"))
          })
        .rethrow
}
