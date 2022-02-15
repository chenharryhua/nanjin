package com.github.chenharryhua.nanjin.pipes

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Framing, Source}
import akka.util.ByteString
import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.terminals.{BUFFER_SIZE, CHUNK_SIZE, NEWLINE_SEPERATOR}
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Pull, Stream}
import kantan.csv.*
import kantan.csv.CsvConfiguration.Header
import kantan.csv.engine.{ReaderEngine, WriterEngine}
import kantan.csv.ops.*
import squants.information.Information

import java.io.{StringReader, StringWriter}

// kudos to authors of https://nrinaudo.github.io/kantan.csv/

object KantanSerde {

  def toBytes[F[_], A](conf: CsvConfiguration, byteBuffer: Information)(implicit
    enc: HeaderEncoder[A],
    F: Async[F]): Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    readOutputStream[F](byteBuffer.toBytes.toInt) { os =>
      def go(as: Stream[F, A], cw: CsvWriter[A]): Pull[F, Unit, Unit] =
        as.pull.uncons.flatMap {
          case Some((hl, tl)) =>
            Pull.eval(F.delay(hl.foldLeft(cw) { case (w, item) => w.write(item) })).flatMap(go(tl, _))
          case None => Pull.eval(F.blocking(cw.close())) >> Pull.done
        }
      go(ss, os.asCsvWriter[A](conf)).stream.compile.drain
    }
  }

  def toBytes[F[_], A](conf: CsvConfiguration)(implicit enc: HeaderEncoder[A], F: Async[F]): Pipe[F, A, Byte] =
    toBytes[F, A](conf, BUFFER_SIZE)

  def fromBytes[F[_], A](conf: CsvConfiguration, chunkSize: ChunkSize)(implicit
    dec: HeaderDecoder[A],
    F: Async[F]): Pipe[F, Byte, A] =
    _.through(toInputStream[F]).flatMap(is =>
      Stream.fromBlockingIterator[F](is.asCsvReader[A](conf).iterator, chunkSize.value).rethrow)

  def fromBytes[F[_], A](conf: CsvConfiguration)(implicit dec: HeaderDecoder[A], F: Async[F]): Pipe[F, Byte, A] =
    fromBytes[F, A](conf, CHUNK_SIZE)

  def rowDecode[A](rowStr: String, conf: CsvConfiguration, dec: RowDecoder[A]): A = {
    val sr: StringReader = new StringReader(rowStr)
    val engine: CsvReader[ReadResult[Seq[String]]] =
      ReaderEngine.internalCsvReaderEngine.readerFor(sr, conf)
    try
      dec.decode(engine.toIndexedSeq.flatMap {
        case Left(ex)     => throw ex
        case Right(value) => value
      }) match {
        case Left(ex)     => throw ex
        case Right(value) => value
      }
    finally engine.close()
  }

  def rowEncode[A](a: A, conf: CsvConfiguration, enc: RowEncoder[A]): String = {
    val sw: StringWriter = new StringWriter
    val engine           = WriterEngine.internalCsvWriterEngine.writerFor(sw, conf).write(enc.encode(a))
    try sw.toString
    finally engine.close()
  }

  def headerStr[A](conf: CsvConfiguration, enc: HeaderEncoder[A]): String = {
    val sw: StringWriter = new StringWriter
    val engine           = WriterEngine.internalCsvWriterEngine.writerFor(sw, conf)

    try {
      if (conf.hasHeader) {
        conf.header match {
          case Header.None             => ()
          case Header.Implicit         => enc.header.foreach(engine.write)
          case Header.Explicit(header) => engine.write(header)
        }
      }

      sw.toString
    } finally engine.close()
  }

  object akka {
    def toByteString[A](conf: CsvConfiguration)(implicit enc: HeaderEncoder[A]): Flow[A, ByteString, NotUsed] =
      Flow[A]
        .map(a => ByteString.fromString(rowEncode(a, conf, enc.rowEncoder)))
        .prepend(Source(List(ByteString.fromString(headerStr(conf, enc)))))

    def fromByteString[A](conf: CsvConfiguration)(implicit dec: HeaderDecoder[A]): Flow[ByteString, A, NotUsed] =
      if (conf.hasHeader)
        Flow[ByteString]
          .via(Framing.delimiter(ByteString.fromString(NEWLINE_SEPERATOR), Int.MaxValue, allowTruncation = true))
          .drop(1)
          .map(bs => rowDecode(bs.utf8String, conf, dec.noHeader))
      else
        Flow[ByteString]
          .via(Framing.delimiter(ByteString.fromString(NEWLINE_SEPERATOR), Int.MaxValue, allowTruncation = true))
          .map(bs => rowDecode(bs.utf8String, conf, dec.noHeader))
  }
}
