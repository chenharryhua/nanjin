package com.github.chenharryhua.nanjin.terminals

import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import cats.effect.kernel.Sync
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.zlib.{ZlibCompressor, ZlibFactory}
import squants.information.{Bytes, Information}

import java.io.{InputStream, OutputStream}
import scala.concurrent.Future

final class NJBytes[F[_]] private (
  cfg: Configuration,
  blockSizeHint: Long,
  bufferSize: Information,
  compressLevel: ZlibCompressor.CompressionLevel)(implicit F: Sync[F]) {
  def withBufferSize(bs: Information): NJBytes[F]               = new NJBytes[F](cfg, blockSizeHint, bs, compressLevel)
  def withBlockSizeHint(bsh: Long): NJBytes[F]                  = new NJBytes[F](cfg, bsh, bufferSize, compressLevel)
  def withCompressionLevel(cl: ZlibCompressor.CompressionLevel) = new NJBytes[F](cfg, blockSizeHint, bufferSize, cl)

  private def inputStream(path: NJPath): InputStream = {
    val is: InputStream = path.hadoopInputFile(cfg).newStream()
    Option(new CompressionCodecFactory(cfg).getCodec(path.hadoopPath)) match {
      case Some(cc) => cc.createInputStream(is)
      case None     => is
    }
  }

  private def outputStream(path: NJPath): OutputStream = {
    ZlibFactory.setCompressionLevel(cfg, compressLevel)
    val os: OutputStream = path.hadoopOutputFile(cfg).createOrOverwrite(blockSizeHint)
    Option(new CompressionCodecFactory(cfg).getCodec(path.hadoopPath)) match {
      case Some(cc) => cc.createOutputStream(os)
      case None     => os
    }
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
  def apply[F[_]: Sync](cfg: Configuration): NJBytes[F] =
    new NJBytes[F](cfg, BlockSizeHint, Bytes(8192), ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION)
}
