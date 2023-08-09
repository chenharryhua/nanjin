package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.KantanSerde
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import kantan.csv.CsvConfiguration
import kantan.csv.generic.*
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.*

class KantanCsvPipeTest extends AnyFunSuite {
  import TestData.*
  val data: Stream[IO, Tiger] = Stream.emits(tigers)
  val hd                      = NJHadoop[IO](new Configuration)

  test("csv identity") {

    assert(
      data
        .through(KantanSerde.toBytes[IO, Tiger](CsvConfiguration.rfc, 300.kb))
        .through(KantanSerde.fromBytes[IO, Tiger](CsvConfiguration.rfc, 5))
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

  val root = NJPath("./data/test/pipes/csv/")
  test("write/read identity csv") {
    val path = root / "csv.csv"
    hd.delete(path).unsafeRunSync()
    val tigers = List(Tiger(1, Some("a|b")), Tiger(2, Some("a'b")), Tiger(3, None), Tiger(4, Some("a||'b")))
    val data   = Stream.emits(tigers).covaryAll[IO, Tiger]
    val rfc = CsvConfiguration.rfc
      .withHeader("a", "b", "c")
      .withCellSeparator('|')
      .withQuote('\'')
      .withQuotePolicy(CsvConfiguration.QuotePolicy.Always)
    val write =
      data.through(KantanSerde.toBytes[IO, Tiger](rfc, 2.kb)).chunks.through(hd.bytes.sink(path))
    val read = hd.bytes.source(path).through(KantanSerde.fromBytes[IO, Tiger](rfc, 1))
    val run  = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }

  test("write/read identity csv header") {
    val path = root / "header.csv"
    hd.delete(path).unsafeRunSync()
    val tigers = List(Tiger(1, Some("a|b")), Tiger(2, Some("a'b")), Tiger(3, None), Tiger(4, Some("a||'b")))
    val data   = Stream.emits(tigers).covaryAll[IO, Tiger]
    val rfc    = CsvConfiguration.rfc.withoutHeader
    val write  = data.through(KantanSerde.toBytes[IO, Tiger](rfc, 2.kb)).chunks.through(hd.bytes.sink(path))
    val read   = hd.bytes.source(path).through(KantanSerde.fromBytes[IO, Tiger](rfc, 1))
    val run    = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tigers)
  }
}
