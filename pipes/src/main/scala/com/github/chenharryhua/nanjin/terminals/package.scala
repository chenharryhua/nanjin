package com.github.chenharryhua.nanjin

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.apache.hadoop.io.compress.zlib.ZlibFactory

import java.io.{InputStream, OutputStream}

package object terminals {
  val BlockSizeHint: Long = -1

  def inputStream(path: NJPath, cfg: Configuration): InputStream = {
    val is: InputStream = path.hadoopInputFile(cfg).newStream()
    Option(new CompressionCodecFactory(cfg).getCodec(path.hadoopPath)) match {
      case Some(cc) => cc.createInputStream(is)
      case None     => is
    }
  }

  def outputStream(path: NJPath, cfg: Configuration, cl: CompressionLevel, blockSizeHint: Long): OutputStream = {
    ZlibFactory.setCompressionLevel(cfg, cl)
    val os: OutputStream = path.hadoopOutputFile(cfg).createOrOverwrite(blockSizeHint)
    Option(new CompressionCodecFactory(cfg).getCodec(path.hadoopPath)) match {
      case Some(cc) => cc.createOutputStream(os)
      case None     => os
    }
  }
}
