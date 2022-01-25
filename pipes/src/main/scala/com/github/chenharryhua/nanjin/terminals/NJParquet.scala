package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import fs2.{Pipe, Pull, Stream}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.ParquetWriter

final class NJParquet[F[_]](implicit F: Sync[F]) {
  def parquetSink(builder: AvroParquetWriter.Builder[GenericRecord]): Pipe[F, GenericRecord, Unit] = {
    def go(grs: Stream[F, GenericRecord], writer: ParquetWriter[GenericRecord]): Pull[F, Unit, Unit] =
      grs.pull.uncons.flatMap {
        case Some((hl, tl)) =>
          Pull.eval(hl.traverse(gr => F.delay(writer.write(gr)))) >> go(tl, writer)
        case None =>
          Pull.eval(F.blocking(writer.close())) >> Pull.done
      }

    (ss: Stream[F, GenericRecord]) =>
      for {
        writer <- Stream.resource(Resource.make(F.blocking(builder.build()))(r => F.blocking(r.close()).attempt.void))
        _ <- go(ss, writer).stream
      } yield ()
  }

  // input path may not exist when eval builder
  def parquetSource(builder: F[AvroParquetReader.Builder[GenericRecord]]): Stream[F, GenericRecord] =
    for {
      reader <- Stream.resource(Resource.make(builder.map(_.build()))(r => F.blocking(r.close()).attempt.void))
      gr <- Stream.repeatEval(F.delay(Option(reader.read()))).unNoneTerminate
    } yield gr
}
