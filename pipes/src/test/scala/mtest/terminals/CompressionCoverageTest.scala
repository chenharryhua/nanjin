package mtest.terminals

import com.github.chenharryhua.nanjin.terminals.*
import com.github.chenharryhua.nanjin.terminals.Compression.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class CompressionCoverageTest extends FunSuite {

  private val ALL: List[Compression] = List(
    Uncompressed,
    Snappy,
    Bzip2,
    Gzip,
    Lz4,
    Lz4Raw,
    Brotli,
    Lzo,
    Deflate(_.Three),
    Xz(_.Four),
    Zstandard(_.Five))

  test("1.fileName covers every FileFormat branch") {
    val expected = List(
      FileFormat.Unknown -> "unknown.unknown.snappy",
      FileFormat.Jackson -> "jackson.json.snappy",
      FileFormat.Circe -> "circe.json.snappy",
      FileFormat.Text -> "plain.txt.snappy",
      FileFormat.Kantan -> "kantan.csv.snappy",
      FileFormat.Parquet -> "snappy.apache.parquet",
      FileFormat.Avro -> "snappy.data.avro",
      FileFormat.BinaryAvro -> "binary.avro.snappy",
      FileFormat.JavaObject -> "java.obj.snappy",
      FileFormat.Protobuf -> "google.pb.snappy"
    )

    expected.foreach { case (format, file_name) =>
      assertEquals(Snappy.fileName(format), file_name)
    }
  }

  test("2.compressionLevel covers fixed compressors and every configurable level") {
    assertEquals(Uncompressed.compressionLevel, CompressionLevel.NO_COMPRESSION)
    List(Snappy, Bzip2, Gzip, Lz4, Lz4Raw, Brotli, Lzo).foreach { compression =>
      assertEquals(compression.compressionLevel, CompressionLevel.DEFAULT_COMPRESSION)
    }

    val expected = List(
      Level.One -> CompressionLevel.BEST_SPEED,
      Level.Two -> CompressionLevel.TWO,
      Level.Three -> CompressionLevel.THREE,
      Level.Four -> CompressionLevel.FOUR,
      Level.Five -> CompressionLevel.FIVE,
      Level.Six -> CompressionLevel.SIX,
      Level.Seven -> CompressionLevel.SEVEN,
      Level.Eight -> CompressionLevel.EIGHT,
      Level.Nine -> CompressionLevel.BEST_COMPRESSION
    )
    expected.foreach { case (level, compression_level) =>
      assertEquals(Deflate(level).compressionLevel, compression_level)
    }
    assertEquals(Xz(_.Two).compressionLevel, CompressionLevel.TWO)
    assertEquals(Zstandard(_.Eight).compressionLevel, CompressionLevel.EIGHT)
  }

  test("3.ParquetCompression maps every supported compression to its codec") {
    val expected = List(
      ParquetCompression.Uncompressed -> CompressionCodecName.UNCOMPRESSED,
      ParquetCompression.Snappy -> CompressionCodecName.SNAPPY,
      ParquetCompression.Gzip -> CompressionCodecName.GZIP,
      ParquetCompression.Lz4 -> CompressionCodecName.LZ4,
      ParquetCompression.Lz4Raw -> CompressionCodecName.LZ4_RAW,
      ParquetCompression.Brotli -> CompressionCodecName.BROTLI,
      ParquetCompression.Lzo -> CompressionCodecName.LZO,
      ParquetCompression.Zstandard(_.Five) -> CompressionCodecName.ZSTD
    )

    expected.foreach { case (compression, codec) => assertEquals(compression.codecName, codec) }
  }

  test("4.AvroCompression builds a codec factory for every supported compression") {
    val compressions = List[AvroCompression](
      AvroCompression.Uncompressed,
      AvroCompression.Snappy,
      AvroCompression.Bzip2,
      AvroCompression.Deflate(_.Three),
      AvroCompression.Xz(_.Four),
      AvroCompression.Zstandard(_.Five)
    )

    compressions.foreach(compression => assert(Option(compression.codecFactory).nonEmpty))
  }

  test("5.base Compression codec round-trips every variant") {
    ALL.foreach(compression => assertEquals(compression.asJson.as[Compression].toOption, Some(compression)))
  }

  test("6.specialized decoders reject recognized but unsupported compression types") {
    val failures = List(
      Json.fromString("gzip").as[AvroCompression].left.toOption.map(_.message),
      Json.fromString("xz-2").as[BinaryAvroCompression].left.toOption.map(_.message),
      Json.fromString("brotli").as[CirceCompression].left.toOption.map(_.message),
      Json.fromString("brotli").as[JacksonCompression].left.toOption.map(_.message),
      Json.fromString("brotli").as[KantanCompression].left.toOption.map(_.message),
      Json.fromString("bzip2").as[ParquetCompression].left.toOption.map(_.message),
      Json.fromString("brotli").as[TextCompression].left.toOption.map(_.message),
      Json.fromString("brotli").as[ProtobufCompression].left.toOption.map(_.message)
    )

    assertEquals(
      failures,
      List(
        Some("avro does not support: Gzip"),
        Some("binary avro does not support: Xz"),
        Some("circe json does not support: Brotli"),
        Some("jackson does not support: Brotli"),
        Some("kantan csv does not support: Brotli"),
        Some("parquet does not support: Bzip2"),
        Some("text does not support: Brotli"),
        Some("protobuf does not support: Brotli")
      )
    )
  }

  test("7.invalid base values and compression levels return descriptive decode failures") {
    assertEquals(
      Json.fromString("unknown").as[Compression].left.toOption.map(_.message),
      Some("unknown compression: unknown"))
    assert(Json.fromString("deflate-a").as[Compression].left.toOption.exists(_.message.nonEmpty))
    assertEquals(
      Json.fromString("deflate-0").as[Compression].left.toOption.map(_.message),
      Some("invalid Compression Level: 0"))
    assertEquals(
      Json.fromString("zstd-10").as[Compression].left.toOption.map(_.message),
      Some("invalid Compression Level: 10"))
  }

  test("8.specialized companion aliases expose their complete supported sets") {
    assertEquals(
      List[AvroCompression](
        AvroCompression.Uncompressed,
        AvroCompression.Snappy,
        AvroCompression.Bzip2,
        AvroCompression.Deflate(_.One),
        AvroCompression.Xz(_.Two),
        AvroCompression.Zstandard(_.Three)
      ).map(_.productPrefix),
      List("Uncompressed", "Snappy", "Bzip2", "Deflate", "Xz", "Zstandard")
    )

    val binary = List[BinaryAvroCompression](
      BinaryAvroCompression.Uncompressed,
      BinaryAvroCompression.Snappy,
      BinaryAvroCompression.Bzip2,
      BinaryAvroCompression.Gzip,
      BinaryAvroCompression.Lz4,
      BinaryAvroCompression.Lz4Raw,
      BinaryAvroCompression.Deflate(_.One)
    )
    val parquet = List[ParquetCompression](
      ParquetCompression.Uncompressed,
      ParquetCompression.Snappy,
      ParquetCompression.Gzip,
      ParquetCompression.Lz4,
      ParquetCompression.Lz4Raw,
      ParquetCompression.Brotli,
      ParquetCompression.Lzo,
      ParquetCompression.Zstandard(_.One)
    )

    assertEquals(binary.size, 7)
    assertEquals(parquet.size, 8)
    assertEquals(JacksonCompression.Deflate(_.One).productPrefix, "Deflate")
    assertEquals(CirceCompression.Deflate(_.One).productPrefix, "Deflate")
    assertEquals(KantanCompression.Deflate(_.One).productPrefix, "Deflate")
    assertEquals(TextCompression.Deflate(_.One).productPrefix, "Deflate")
    assertEquals(ProtobufCompression.Deflate(_.One).productPrefix, "Deflate")
  }
}
