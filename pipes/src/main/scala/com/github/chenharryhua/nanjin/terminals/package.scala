package com.github.chenharryhua.nanjin

import com.github.chenharryhua.nanjin.common.ChunkSize
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.apache.hadoop.io.compress.zlib.ZlibFactory
import squants.information.{Bytes, Information}

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets
import fs2.{Pull, Stream}
import org.apache.avro.generic.GenericRecord
package object terminals {
  final val NEWLINE_SEPERATOR: String            = "\r\n"
  final val NEWLINE_BYTES_SEPERATOR: Array[Byte] = NEWLINE_SEPERATOR.getBytes(StandardCharsets.UTF_8)

  final val BLOCK_SIZE_HINT: Long    = -1
  final val BUFFER_SIZE: Information = Bytes(8192)
  final val CHUNK_SIZE: ChunkSize    = ChunkSize(1000)

  def fileInputStream(path: NJPath, configuration: Configuration): InputStream = {
    val is: InputStream = path.hadoopInputFile(configuration).newStream()
    Option(new CompressionCodecFactory(configuration).getCodec(path.hadoopPath)) match {
      case Some(cc) => cc.createInputStream(is)
      case None     => is
    }
  }

  def fileOutputStream(
    path: NJPath,
    configuration: Configuration,
    compressionLevel: CompressionLevel,
    blockSizeHint: Long): OutputStream = {
    ZlibFactory.setCompressionLevel(configuration, compressionLevel)
    val os: OutputStream = path.hadoopOutputFile(configuration).createOrOverwrite(blockSizeHint)
    Option(new CompressionCodecFactory(configuration).getCodec(path.hadoopPath)) match {
      case Some(cc) => cc.createOutputStream(os)
      case None     => os
    }
  }

  private[terminals] def persistGenericRecord[F[_]](
    grs: Stream[F, GenericRecord],
    writer: NJWriter[F, GenericRecord]): Pull[F, Nothing, Unit] =
    grs.pull.uncons.flatMap {
      case Some((hl, tl)) => Pull.eval(writer.write(hl)) >> persistGenericRecord(tl, writer)
      case None           => Pull.done
    }
}
