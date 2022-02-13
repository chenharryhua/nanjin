package com.github.chenharryhua.nanjin

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.apache.hadoop.io.compress.zlib.ZlibFactory

import java.io.{InputStream, OutputStream}

package object terminals {
  val BlockSizeHint: Long = -1

  def inputStream(path: NJPath, configuration: Configuration): InputStream = {
    val is: InputStream = path.hadoopInputFile(configuration).newStream()
    Option(new CompressionCodecFactory(configuration).getCodec(path.hadoopPath)) match {
      case Some(cc) => cc.createInputStream(is)
      case None     => is
    }
  }

  def outputStream(
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
}
