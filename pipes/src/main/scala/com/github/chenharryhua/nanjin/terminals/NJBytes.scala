package com.github.chenharryhua.nanjin.terminals

import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import cats.effect.kernel.Sync
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodecFactory
import squants.information.{Bytes, Information}

import java.io.{InputStream, OutputStream}
import scala.concurrent.Future

final class NJBytes[F[_]] private (cfg: Configuration, blockSizeHint: Long, bufferSize: Information)(implicit
  F: Sync[F]) {
  def withBufferSize(bs: Information): NJBytes[F] = new NJBytes[F](cfg, blockSizeHint, bs)
  def withBlockSizeHint(bsh: Long): NJBytes[F]    = new NJBytes[F](cfg, bsh, bufferSize)

  private def inputStream(path: NJPath): InputStream =
    Option(new CompressionCodecFactory(cfg).getCodec(path.hadoopPath)) match {
      case Some(cc) => cc.createInputStream(path.hadoopInputFile(cfg).newStream())
      case None     => path.hadoopInputFile(cfg).newStream()
    }

  private def outputStream(path: NJPath): OutputStream =
    Option(new CompressionCodecFactory(cfg).getCodec(path.hadoopPath)) match {
      case Some(cc) => cc.createOutputStream(path.hadoopOutputFile(cfg).createOrOverwrite(blockSizeHint))
      case None     => path.hadoopOutputFile(cfg).createOrOverwrite(blockSizeHint)
    }

  def source(path: NJPath): Stream[F, Byte] =
    for {
      is <- Stream.bracket(F.blocking(inputStream(path)))(r => F.blocking(r.close()))
      byte <- readInputStream[F](F.pure(is), bufferSize.toBytes.toInt, closeAfterUse = false) // avoid double close
    } yield byte

  def sink(path: NJPath): Pipe[F, Byte, Unit] = { (ss: Stream[F, Byte]) =>
    Stream
      .bracket(F.blocking(outputStream(path)))(r => F.blocking(r.close()))
      .flatMap(os => ss.through(writeOutputStream(F.pure(os), closeAfterUse = false))) // avoid double close
  }

  object akka {
    def source(path: NJPath): Source[ByteString, Future[IOResult]] =
      StreamConverters.fromInputStream(() => inputStream(path))

    def sink(path: NJPath): Sink[ByteString, Future[IOResult]] =
      StreamConverters.fromOutputStream(() => outputStream(path))
  }
}

object NJBytes {
  def apply[F[_]: Sync](cfg: Configuration): NJBytes[F] = new NJBytes[F](cfg, BlockSizeHint, Bytes(8192))
}
