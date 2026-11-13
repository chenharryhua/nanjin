package mtest.terminals

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.pipes.jackson
import com.github.chenharryhua.nanjin.terminals.given
import com.github.chenharryhua.nanjin.terminals.{CreateRotateFile, RotateFile}
import fs2.Stream
import io.circe.Json
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import org.apache.avro.generic.{GenericData, GenericRecord}

import java.time.{Instant, ZonedDateTime}
import java.util.UUID

/** Tests covering behaviors flagged during module inspection that lacked direct coverage:
  *   - the hand-written `RotateFile` circe codec wire contract,
  *   - the ISO-8859-1 charset pairing in `jackson.toBytes`/`fromBytes`,
  *   - the first-record schema assumption of the schema-less rotating sinks.
  */
class InspectionGapsTest extends CatsEffectSuite {
  import HadoopTestData.*

  private val root: Url = Url.parse("./data/test/terminals/inspection-gaps")

  private def sampleRotateFile: RotateFile = {
    val zone = sydneyTime
    val create = CreateRotateFile(
      sequenceId = UUID.fromString("12345678-90ab-cdef-1234-567890abcdef"),
      index = 7L,
      time = ZonedDateTime.ofInstant(Instant.ofEpochSecond(1_700_000_000L), zone)
    )
    RotateFile(
      create = create,
      closed = Instant.ofEpochSecond(1_700_000_123L),
      url = Url.parse("./data/test/terminals/inspection-gaps/00007.circe"),
      recordCount = 42L
    )
  }

  // -------------------- RotateFile codec wire contract --------------------

  test("1.RotateFile circe codec round-trips through its hand-written encoder/decoder") {
    val original = sampleRotateFile
    val decoded = original.asJson.as[RotateFile]

    decoded match {
      case Right(rf) =>
        assertEquals(rf.url, original.url)
        assertEquals(rf.recordCount, original.recordCount)
        assertEquals(rf.create.index, original.create.index)
        assertEquals(rf.create.sequenceId, original.create.sequenceId)
        // Times survive the LocalDateTime+zoneId round-trip to the same instant.
        assertEquals(rf.create.time.toInstant, original.create.time.toInstant)
        assertEquals(rf.closed, original.closed)
        assertEquals(rf.create.time.getZone, original.create.time.getZone)
      case Left(err) => fail(s"decode failed: $err")
    }
  }

  test("2.RotateFile encoder emits exactly the documented wire keys") {
    val json = sampleRotateFile.asJson
    val keys = json.asObject.map(_.keys.toSet).getOrElse(Set.empty)
    // These keys are a persisted wire contract; a rename is a breaking change.
    assertEquals(
      keys,
      Set("index", "url", "recordCount", "create", "closed", "window", "zoneId", "sequenceId"))
  }

  test("3.RotateFile window is write-only: encoded as a string, derived (not read) on decode") {
    val json = sampleRotateFile.asJson
    // window is present and rendered as a formatted string.
    assert(json.hcursor.get[String]("window").isRight, "window should encode as a string")
    // Dropping window from the JSON still decodes, because the decoder derives it from create/closed.
    val withoutWindow = json.mapObject(_.remove("window"))
    withoutWindow.as[RotateFile] match {
      case Right(rf) =>
        assertEquals(rf.window, java.time.Duration.between(rf.create.time.toInstant, rf.closed))
      case Left(err) => fail(s"decode without window failed: $err")
    }
  }

  // -------------------- jackson pipe charset pairing --------------------

  test("4.jackson toBytes/fromBytes round-trips non-ASCII string data") {
    // A record whose string field holds multibyte characters. jackson.toBytes emits line bytes as
    // ISO-8859-1 to match Avro's JsonEncoder; fromBytes must read them back consistently.
    val record = new GenericData.Record(pandaSchema)
    record.put("name", "résumé-日本語")
    record.put("age", 8)
    record.put("id", 12345)

    val roundTrip: IO[List[GenericRecord]] =
      Stream
        .emit[IO, GenericRecord](record)
        .through(jackson.toBytes[IO](pandaSchema))
        .through(jackson.fromBytes[IO](pandaSchema))
        .compile
        .toList

    roundTrip.map { records =>
      assertEquals(records.size, 1)
      assertEquals(records.head.get("name").toString, "résumé-日本語")
      assertEquals(records.head.get("age").toString, "8")
      assertEquals(records.head.get("id").toString, "12345")
    }
  }

  // -------------------- schema-less sink: first-record schema assumption --------------------

  test("5.schema-less binAvro sink derives its schema from the first record") {
    // The schema-less rotating sinks peek the first record for the Avro schema and assume every
    // subsequent record shares it. Feed a single-record stream and confirm the file is written and
    // round-trips under that first record's schema.
    val path = root / "schema-peek"
    val record = new GenericData.Record(pandaSchema)
    record.put("name", "zhouzhou")
    record.put("age", 8)
    record.put("id", 999)

    val sink = hdp.rotateSink(sydneyTime, 10L)(t => path / s"${t.index}.bin.avro").binAvro

    val run = hdp.delete(path) >> Stream
      .emit[IO, GenericRecord](record)
      .through(sink)
      .compile
      .toList

    run.flatMap { rotateFiles =>
      assertEquals(rotateFiles.size, 1)
      assertEquals(rotateFiles.head.recordCount, 1L)
      hdp
        .source(rotateFiles.head.url)
        .binAvro(100, pandaSchema)
        .compile
        .toList
        .map { readBack =>
          assertEquals(readBack.size, 1)
          assertEquals(readBack.head.get("name").toString, "zhouzhou")
        }
    }
  }

  // -------------------- circe source: chunk sizing --------------------

  /** 1,000 tiny values fit in one 128 KiB read, so a single `absorb` parses far more than `chunkSize`. Before
    * the carry-over was drained first, the next step split the newly parsed values at a negative index, and
    * fs2 answers that with `(empty, this)` - so the second chunk carried everything left over.
    */
  private def chunkSizes(name: String, chunkSize: Int): IO[List[Int]] = {
    val path = root / "chunking" / name
    val data: Stream[IO, Json] = Stream.emits(1 to 1000).map(i => Json.obj("i" -> Json.fromInt(i)))
    hdp.delete(path) >>
      data.through(hdp.sink(path).circe).compile.drain >>
      hdp.source(path).circe(chunkSize).chunks.map(_.size).compile.toList
  }

  test("6.circe source emits exactly chunkSize values per chunk when the size divides evenly") {
    chunkSizes("even.json", 10).map { sizes =>
      assertEquals(sizes.sum, 1000)
      assertEquals(sizes, List.fill(100)(10))
    }
  }

  test("7.circe source emits a short final chunk and full chunks before it") {
    chunkSizes("uneven.json", 7).map { sizes =>
      assertEquals(sizes.sum, 1000)
      assertEquals(sizes.init, List.fill(142)(7))
      assertEquals(sizes.last, 6)
    }
  }

  // -------------------- Url codec: a URI that is not a URL --------------------

  test("8.Url decoder fails a URN instead of throwing out of the decoder") {
    // scala-uri's Urn.toUrl throws UriConversionException, which circe does not catch, so before the
    // decoder guarded the conversion this call threw rather than returning a Left.
    val result = decode[Url](""""urn:isbn:0451450523"""")
    assert(result.isLeft, result)
    assert(result.left.exists(_.getMessage.contains("not a url")), result)
  }

  test("9.Url decoder still accepts a url") {
    assertEquals(decode[Url](""""s3://bucket/key.json""""), Right(Url.parse("s3://bucket/key.json")))
  }
}
