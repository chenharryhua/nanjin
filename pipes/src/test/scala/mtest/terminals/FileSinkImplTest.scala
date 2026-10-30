package mtest.terminals

import cats.effect.IO
import fs2.{Chunk, Stream}
import io.circe.Json
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import kantan.csv.CsvConfiguration
import mtest.terminals.HadoopTestData.*
import munit.CatsEffectSuite
import org.apache.avro.generic.GenericRecord

class FileSinkImplTest extends CatsEffectSuite {

  private val ROOT: Url = Url.parse("./data/test/terminals/file-sink-impl")

  test("1.empty GenericRecord streams exercise every schema-derived sink without creating a writer") {
    def empty = Stream.empty.covaryAll[IO, GenericRecord]

    for {
      _ <- hdp.delete(ROOT / "empty")
      avro <- empty.through(hdp.sink(ROOT / "empty" / "data.avro").avro).compile.toList
      binary <- empty.through(hdp.sink(ROOT / "empty" / "binary.avro").binAvro).compile.toList
      jackson <- empty.through(hdp.sink(ROOT / "empty" / "jackson.json").jackson).compile.toList
      parquet <- empty.through(hdp.sink(ROOT / "empty" / "apache.parquet").parquet).compile.toList
    } yield {
      assertEquals(avro, Nil)
      assertEquals(binary, Nil)
      assertEquals(jackson, Nil)
      assertEquals(parquet, Nil)
    }
  }

  test("2.Kantan function overload applies its transformation to the RFC configuration") {
    val function_path = ROOT / "kantan" / "function.csv"
    val explicit_path = ROOT / "kantan" / "explicit.csv"
    val rows = List(Seq("one", "two"), Seq("three", "four"))
    val configure = (configuration: CsvConfiguration) => configuration.withCellSeparator('|')

    for {
      _ <- hdp.delete(ROOT / "kantan")
      function_counts <- Stream.emits(rows).covary[IO]
        .through(hdp.sink(function_path).kantan(configure))
        .compile
        .toList
      explicit_counts <- Stream.emits(rows).covary[IO]
        .through(hdp.sink(explicit_path).kantan(configure(CsvConfiguration.rfc)))
        .compile
        .toList
      function_rows <- hdp.source(function_path).kantan(1, configure).compile.toList
      explicit_rows <- hdp.source(explicit_path).kantan(1, configure(CsvConfiguration.rfc)).compile.toList
    } yield {
      assertEquals(function_counts.sum, rows.size)
      assertEquals(explicit_counts.sum, rows.size)
      assertEquals(function_rows, rows)
      assertEquals(explicit_rows, rows)
    }
  }

  test("3.text, bytes, Circe, and Kantan sinks emit one count per input chunk") {
    val text = Stream.chunk(Chunk("one", "two")) ++ Stream.emit("three")
    val bytes = Stream.chunk(Chunk[Byte](1, 2, 3)) ++ Stream.emit(4.toByte)
    val json = Stream.chunk(Chunk(Json.fromInt(1), Json.fromInt(2))) ++ Stream.emit(Json.fromInt(3))
    val rows = Stream.chunk(Chunk(Seq("a"), Seq("b"))) ++ Stream.emit(Seq("c"))

    for {
      _ <- hdp.delete(ROOT / "counts")
      text_counts <- text.covary[IO].through(hdp.sink(ROOT / "counts" / "text.txt").text).compile.toList
      byte_counts <- bytes.covary[IO].through(hdp.sink(ROOT / "counts" / "bytes.bin").bytes).compile.toList
      json_counts <- json.covary[IO].through(hdp.sink(ROOT / "counts" / "circe.json").circe).compile.toList
      row_counts <- rows.covary[IO].through(hdp.sink(ROOT / "counts" / "kantan.csv").kantan).compile.toList
    } yield {
      assertEquals(text_counts, List(2, 1))
      assertEquals(byte_counts, List(3, 1))
      assertEquals(json_counts, List(2, 1))
      assertEquals(row_counts, List(2, 1))
    }
  }

  test("4.schema-derived sinks preserve input chunk counts") {
    val records = Stream.chunk(Chunk.from(pandas)) ++ Stream.emit(pandas.head)

    for {
      _ <- hdp.delete(ROOT / "generic-counts")
      avro <- records.covary[IO]
        .through(hdp.sink(ROOT / "generic-counts" / "data.avro").avro)
        .compile
        .toList
      binary <- records.covary[IO]
        .through(hdp.sink(ROOT / "generic-counts" / "binary.avro").binAvro)
        .compile
        .toList
      jackson <- records.covary[IO]
        .through(hdp.sink(ROOT / "generic-counts" / "jackson.json").jackson)
        .compile
        .toList
      parquet <- records.covary[IO]
        .through(hdp.sink(ROOT / "generic-counts" / "apache.parquet").parquet)
        .compile
        .toList
    } yield {
      assertEquals(avro, List(2, 1))
      assertEquals(binary, List(2, 1))
      assertEquals(jackson, List(2, 1))
      assertEquals(parquet, List(2, 1))
    }
  }
}
