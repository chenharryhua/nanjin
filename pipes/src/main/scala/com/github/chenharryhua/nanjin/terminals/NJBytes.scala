package com.github.chenharryhua.nanjin.terminals

import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import cats.effect.kernel.Sync
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{INothing, Pipe, Stream}
import io.scalaland.enumz.Enum
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import squants.information.{Bytes, Information}

import scala.concurrent.Future

final class NJBytes[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  bufferSize: Information,
  compressLevel: CompressionLevel)(implicit F: Sync[F]) {
  def withBufferSize(bs: Information): NJBytes[F]  = new NJBytes[F](configuration, blockSizeHint, bs, compressLevel)
  def withBlockSizeHint(bsh: Long): NJBytes[F]     = new NJBytes[F](configuration, bsh, bufferSize, compressLevel)
  def withCompressionLevel(cl: CompressionLevel)   = new NJBytes[F](configuration, blockSizeHint, bufferSize, cl)
  def withCompressionLevel(level: Int): NJBytes[F] = withCompressionLevel(Enum[CompressionLevel].withIndex(level))

  def source(path: NJPath): Stream[F, Byte] =
    for {
      is <- Stream.bracket(F.blocking(inputStream(path, configuration)))(r => F.blocking(r.close()))
      byte <- readInputStream[F](F.pure(is), bufferSize.toBytes.toInt, closeAfterUse = false) // avoid double close
    } yield byte

  def sink(path: NJPath): Pipe[F, Byte, INothing] = { (ss: Stream[F, Byte]) =>
    Stream
      .bracket(F.blocking(outputStream(path, configuration, compressLevel, blockSizeHint)))(r => F.blocking(r.close()))
      .flatMap(os => ss.through(writeOutputStream(F.pure(os), closeAfterUse = false))) // avoid double close
  }

  object akka {
    def source(path: NJPath): Source[ByteString, Future[IOResult]] =
      StreamConverters.fromInputStream(() => inputStream(path, configuration))

    def sink(path: NJPath): Sink[ByteString, Future[IOResult]] =
      StreamConverters.fromOutputStream(() => outputStream(path, configuration, compressLevel, blockSizeHint))
  }
}

object NJBytes {
  def apply[F[_]: Sync](cfg: Configuration): NJBytes[F] =
    new NJBytes[F](cfg, BlockSizeHint, Bytes(8192), CompressionLevel.DEFAULT_COMPRESSION)
}
