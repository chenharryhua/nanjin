package com.github.chenharryhua.nanjin.terminals

import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import akka.util.ByteString
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.io.{readInputStream, writeOutputStream}
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory}

import java.io.{InputStream, OutputStream}
import scala.concurrent.Future

final class NJBytes[F[_]] private (
  cfg: Configuration,
  compressionCodec: Option[CompressionCodec],
  blockSizeHint: Long,
  chunkSize: ChunkSize)(implicit F: Sync[F]) {
  def withCompressionCodec(codec: CompressionCodec): NJBytes[F] =
    new NJBytes[F](cfg, Some(codec), blockSizeHint, chunkSize)
  def withCompressionCodec(codec: Option[CompressionCodec]): NJBytes[F] =
    new NJBytes[F](cfg, codec, blockSizeHint, chunkSize)
  def withChunkSize(cs: ChunkSize): NJBytes[F]  = new NJBytes[F](cfg, compressionCodec, blockSizeHint, cs)
  def withBlockSizeHint(size: Long): NJBytes[F] = new NJBytes[F](cfg, compressionCodec, size, chunkSize)

  def source(path: NJPath): Stream[F, Byte] =
    for {
      hif <- Stream(path.hadoopInputFile(cfg))
      is: InputStream <- Stream.bracket(F.blocking(hif.newStream()))(r => F.blocking(r.close()))
      compressed: F[InputStream] = {
        val factory = new CompressionCodecFactory(cfg)
        compressionCodec match {
          case Some(cc) => F.blocking(factory.getCodecByClassName(cc.getClass.getName).createInputStream(is))
          case None =>
            Option(factory.getCodec(hif.getPath)) match {
              case Some(cc) => F.blocking(cc.createInputStream(is))
              case None     => F.pure(is)
            }
        }
      }
      byte <- readInputStream[F](compressed, chunkSize = chunkSize.value, closeAfterUse = true)
    } yield byte

  def sink(path: NJPath): Pipe[F, Byte, Unit] = {
    val output = path.hadoopOutputFile(cfg)
    def compressOutputStream(os: OutputStream): OutputStream =
      compressionCodec.fold(os) { codec =>
        val factory       = new CompressionCodecFactory(cfg)
        val compressCodec = factory.getCodecByClassName(codec.getClass.getName)
        require( // extension consistency check
          factory.getCodec(new Path(output.getPath)) == compressCodec,
          s"${path.pathStr} should have extension ${codec.getDefaultExtension}"
        )
        compressCodec.createOutputStream(os)
      }

    (ss: Stream[F, Byte]) =>
      Stream
        .bracket(F.blocking(output.createOrOverwrite(blockSizeHint)))(r => F.blocking(r.close()))
        .map(compressOutputStream)
        .flatMap(out => ss.through(writeOutputStream(F.pure(out))))
  }

  object akka {
    def source(path: NJPath): Source[ByteString, Future[IOResult]] =
      StreamConverters.fromInputStream(() => path.hadoopInputFile(cfg).newStream())

    def sink(path: NJPath): Sink[ByteString, Future[IOResult]] =
      StreamConverters.fromOutputStream(() => path.hadoopOutputFile(cfg).createOrOverwrite(blockSizeHint))
  }
}

object NJBytes {
  def apply[F[_]: Sync](cfg: Configuration): NJBytes[F] = new NJBytes[F](cfg, None, -1L, ChunkSize(8192))
}
