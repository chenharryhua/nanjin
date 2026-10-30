package mtest.terminals

import com.github.chenharryhua.nanjin.terminals.*
import io.circe.syntax.EncoderOps
import munit.FunSuite

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.UUID

class FileKindTest extends FunSuite {

  private val CASES: List[(FileKind, FileFormat, String)] = List(
    (AvroFile(_.Uncompressed), FileFormat.Avro, "uncompressed.data.avro"),
    (CirceFile(_.Uncompressed), FileFormat.Circe, "circe.json"),
    (BinAvroFile(_.Uncompressed), FileFormat.BinaryAvro, "binary.avro"),
    (JacksonFile(_.Uncompressed), FileFormat.Jackson, "jackson.json"),
    (KantanFile(_.Uncompressed), FileFormat.Kantan, "kantan.csv"),
    (ParquetFile(_.Uncompressed), FileFormat.Parquet, "uncompressed.apache.parquet"),
    (ProtobufFile(_.Uncompressed), FileFormat.Protobuf, "google.pb"),
    (TextFile(_.Uncompressed), FileFormat.Text, "plain.txt")
  )

  private val TIME = LocalDateTime.of(2026, 1, 2, 3, 4, 5)
  private val CREATE = CreateRotateFile(
    sequenceId = UUID.fromString("12345678-90ab-cdef-1234-567890abcdef"),
    index = 7L,
    time = ZonedDateTime.of(TIME, ZoneId.of("UTC")))

  test("1.every FileKind carries its format and expected uncompressed file name") {
    CASES.foreach { case (kind, format, file_name) =>
      assertEquals(kind.fileFormat, format)
      assertEquals(kind.fileName, file_name)
    }
  }

  test("2.function-based companion constructors apply their compression selection") {
    val compressed: List[FileKind] = List(
      AvroFile(_.Snappy),
      CirceFile(_.Snappy),
      BinAvroFile(_.Snappy),
      JacksonFile(_.Snappy),
      KantanFile(_.Snappy),
      ParquetFile(_.Snappy),
      ProtobufFile(_.Snappy),
      TextFile(_.Snappy)
    )

    assert(compressed.forall(_.compression == Compression.Snappy))
    assert(compressed.forall(_.fileName.contains("snappy")))
  }

  test("3.LocalDateTime file names use yyyyMMdd-HHmmss before the format name") {
    CASES.foreach { case (kind, _, file_name) =>
      assertEquals(kind.fileName(TIME), s"20260102-030405.$file_name")
    }
  }

  test("4.rotation file names use UUID prefix, zero-padded index, local time, and format name") {
    CASES.foreach { case (kind, _, file_name) =>
      assertEquals(kind.fileName(CREATE), s"12345-0007-030405.$file_name")
    }
  }

  test("5.ymdFileName prefixes the rotation name with the partition date path") {
    CASES.foreach { case (kind, _, file_name) =>
      assertEquals(kind.ymdFileName(CREATE), s"Year=2026/Month=01/Day=02/12345-0007-030405.$file_name")
    }
  }

  test("6.every FileKind subtype round-trips through the derived codec") {
    val kinds = CASES.map(_._1) ::: List[FileKind](
      AvroFile(_.Deflate(_.Nine)),
      CirceFile(_.Gzip),
      BinAvroFile(_.Bzip2),
      JacksonFile(_.Lz4),
      KantanFile(_.Deflate(_.Three)),
      ParquetFile(_.Zstandard(_.Five)),
      ProtobufFile(_.Gzip),
      TextFile(_.Bzip2)
    )

    kinds.foreach(kind => assertEquals(kind.asJson.as[FileKind].toOption, Some(kind)))
  }
}
