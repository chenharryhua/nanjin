package mtest.terminals

import cats.effect.IO
import com.github.chenharryhua.nanjin.terminals.FileSource
import fs2.Stream
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import kantan.csv.CsvConfiguration
import mtest.terminals.HadoopTestData.*
import mtest.terminals.proto.test_message.TestAnimal
import munit.CatsEffectSuite
import squants.information.{Bits, Bytes}

/** Focused API tests for `FileSource`.
  *
  * The format-specific suites cover full read/write behavior and compression. This suite pins behavior shared
  * by the `FileSource` facade: every decoded-value reader validates its chunk size eagerly, raw-byte buffers
  * enforce the one-byte boundary, construction remains lazy for missing paths, and convenience overloads
  * produce the same values as their explicit counterparts.
  */
class FileSourceTest extends CatsEffectSuite {

  private val ROOT: Url = Url.parse("./data/test/terminals/file-source")

  private def invalid_chunk_builders(source: FileSource[IO], chunk_size: Int): List[(String, () => Any)] =
    List(
      "avro(writer schema)" -> (() => source.avro(chunk_size)),
      "avro(reader schema)" -> (() => source.avro(chunk_size, readerSchema)),
      "binAvro(single schema)" -> (() => source.binAvro(chunk_size, pandaSchema)),
      "binAvro(writer and reader schemas)" -> (() => source.binAvro(chunk_size, pandaSchema, readerSchema)),
      "jackson(single schema)" -> (() => source.jackson(chunk_size, pandaSchema)),
      "jackson(writer and reader schemas)" -> (() => source.jackson(chunk_size, pandaSchema, readerSchema)),
      "circe" -> (() => source.circe(chunk_size)),
      "kantan(default)" -> (() => source.kantan(chunk_size)),
      "kantan(configuration)" -> (() => source.kantan(chunk_size, CsvConfiguration.rfc)),
      "kantan(configuration function)" -> (() =>
        source.kantan(chunk_size, (cfg: CsvConfiguration) => cfg.withHeader(false))),
      "parquet(default)" -> (() => source.parquet(chunk_size)),
      "parquet(builder function)" -> (() => source.parquet(chunk_size, identity)),
      "text" -> (() => source.text(chunk_size)),
      "protobuf" -> (() => source.protobuf[TestAnimal](chunk_size)),
      "jsonNode" -> (() => source.jsonNode(chunk_size, objectMapper.reader()))
    )

  private def positive_builders(source: FileSource[IO]): List[(String, () => Any)] =
    invalid_chunk_builders(source, 1) :+ ("bytes" -> (() => source.bytes(Bytes(1))))

  test("1.every decoded-value reader rejects zero and negative chunk sizes eagerly") {
    val source = hdp.source(ROOT / "missing")

    List(0, -1).foreach { chunk_size =>
      invalid_chunk_builders(source, chunk_size).foreach { case (name, build) =>
        val error = intercept[IllegalArgumentException](build())
        assert(
          error.getMessage.contains(s"but was $chunk_size"),
          s"$name returned an unexpected validation message: ${error.getMessage}")
      }
    }
  }

  test("2.bytes rejects sub-byte buffers and accepts exactly one byte") {
    val source = hdp.source(ROOT / "missing.bin")
    val error = intercept[IllegalArgumentException](source.bytes(Bits(7)))

    assert(error.getMessage.contains("at least 1 byte"))
    assert(Option(source.bytes(Bytes(1))).nonEmpty)
  }

  test("3.every reader is lazy when constructed for a missing path") {
    val source = hdp.source(ROOT / "does-not-exist")

    positive_builders(source).foreach { case (name, build) =>
      assert(Option(build()).nonEmpty, s"$name failed during stream construction")
    }
  }

  test("4.binAvro and jackson shorthand overloads match explicit writer/reader schemas") {
    val bin_path = ROOT / "overloads" / "binary.avro"
    val jackson_path = ROOT / "overloads" / "jackson.json"
    val records = Stream.emits(pandas).covary[IO]

    for {
      _ <- hdp.delete(ROOT / "overloads")
      _ <- records.through(hdp.sink(bin_path).binAvro).compile.drain
      bin_short <- hdp.source(bin_path).binAvro(1, pandaSchema).compile.toList
      bin_explicit <- hdp.source(bin_path).binAvro(1, pandaSchema, pandaSchema).compile.toList
      _ <- records.through(hdp.sink(jackson_path).jackson).compile.drain
      jackson_short <- hdp.source(jackson_path).jackson(1, pandaSchema).compile.toList
      jackson_explicit <- hdp.source(jackson_path).jackson(1, pandaSchema, pandaSchema).compile.toList
    } yield {
      assertEquals(bin_short.toSet, bin_explicit.toSet)
      assertEquals(bin_short.toSet, pandaSet)
      assertEquals(jackson_short.toSet, jackson_explicit.toSet)
      assertEquals(jackson_short.toSet, pandaSet)
    }
  }

  test("5.parquet and kantan convenience overloads match their explicit forms") {
    val parquet_path = ROOT / "overloads" / "apache.parquet"
    val csv_path = ROOT / "overloads" / "kantan.csv"
    val records = Stream.emits(pandas).covary[IO]
    val rows = List(Seq("1", "panda"), Seq("2", "tiger"))

    for {
      _ <- hdp.delete(ROOT / "overloads")
      _ <- records.through(hdp.sink(parquet_path).parquet).compile.drain
      parquet_default <- hdp.source(parquet_path).parquet(1).compile.toList
      parquet_explicit <- hdp.source(parquet_path).parquet(1, identity).compile.toList
      _ <- Stream.emits(rows).covary[IO].through(hdp.sink(csv_path).kantan).compile.drain
      kantan_default <- hdp.source(csv_path).kantan(1).compile.toList
      kantan_configuration <- hdp.source(csv_path).kantan(1, CsvConfiguration.rfc).compile.toList
      kantan_function <- hdp
        .source(csv_path)
        .kantan(1, (cfg: CsvConfiguration) => cfg)
        .compile
        .toList
    } yield {
      assertEquals(parquet_default.toSet, parquet_explicit.toSet)
      assertEquals(parquet_default.toSet, pandaSet)
      assertEquals(kantan_default, rows)
      assertEquals(kantan_configuration, rows)
      assertEquals(kantan_function, rows)
    }
  }
}
