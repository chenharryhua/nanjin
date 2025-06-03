package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.terminals.Compression
import org.apache.avro.file.DataFileConstants
import org.apache.avro.mapred.AvroOutputFormat
import org.apache.avro.mapreduce.AvroJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.*
import org.apache.hadoop.io.compress.zlib.ZlibFactory
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.codec.ZstandardCodec
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.catalyst.util.CompressionCodecs

private[persist] object compressionConfig {

  final def avro(conf: Configuration, compression: Compression): Unit =
    compression match {
      case Compression.Uncompressed =>
        conf.set(FileOutputFormat.COMPRESS, "false")
        conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.NULL_CODEC)
      case Compression.Snappy =>
        conf.set(FileOutputFormat.COMPRESS, "true")
        conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC)
      case Compression.Bzip2 =>
        conf.set(FileOutputFormat.COMPRESS, "true")
        conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.BZIP2_CODEC)
      case Compression.Deflate(v) =>
        conf.set(FileOutputFormat.COMPRESS, "true")
        conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC)
        conf.set(AvroOutputFormat.DEFLATE_LEVEL_KEY, v.toString)
      case Compression.Xz(v) =>
        conf.set(FileOutputFormat.COMPRESS, "true")
        conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.XZ_CODEC)
        conf.set(AvroOutputFormat.XZ_LEVEL_KEY, v.toString)
      case c => sys.error(s"not support ${c.productPrefix} in avro")
    }

  final def parquet(conf: Configuration, compression: Compression): CompressionCodecName =
    compression match {
      case Compression.Uncompressed     => CompressionCodecName.UNCOMPRESSED
      case Compression.Snappy           => CompressionCodecName.SNAPPY
      case Compression.Gzip             => CompressionCodecName.GZIP
      case Compression.Lz4              => CompressionCodecName.LZ4
      case Compression.Lz4_Raw          => CompressionCodecName.LZ4_RAW
      case Compression.Brotli           => CompressionCodecName.BROTLI
      case Compression.Zstandard(level) =>
        conf.set(ZstandardCodec.PARQUET_COMPRESS_ZSTD_LEVEL, level.toString)
        CompressionCodecName.ZSTD
      case c => sys.error(s"not support ${c.productPrefix} in parquet")
    }

  final def set(config: Configuration, compression: Compression): Unit =
    compression match {
      case Compression.Uncompressed => CompressionCodecs.setCodecConfiguration(config, null)
      case Compression.Snappy       =>
        CompressionCodecs.setCodecConfiguration(config, classOf[SnappyCodec].getName)
      case Compression.Bzip2 => CompressionCodecs.setCodecConfiguration(config, classOf[BZip2Codec].getName)
      case Compression.Gzip  => CompressionCodecs.setCodecConfiguration(config, classOf[GzipCodec].getName)
      case Compression.Lz4   => CompressionCodecs.setCodecConfiguration(config, classOf[Lz4Codec].getName)
      case Compression.Deflate(_) =>
        ZlibFactory.setCompressionLevel(config, compression.compressionLevel)
        CompressionCodecs.setCodecConfiguration(config, classOf[DeflateCodec].getName)
      case Compression.Zstandard(_) =>
        ZlibFactory.setCompressionLevel(config, compression.compressionLevel)
        CompressionCodecs.setCodecConfiguration(config, classOf[ZStandardCodec].getName)
      case cc => sys.error(s"${cc.shortName} is not supported")
    }
}
